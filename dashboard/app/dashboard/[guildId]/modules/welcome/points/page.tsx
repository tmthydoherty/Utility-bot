import type { Metadata } from "next";

import { isAvailable, readLists, readMembers, readMeta } from "@/lib/onboarding/store";
import { onboardingTile } from "@/lib/onboarding/tabs";
import { OnboardingSectionFrame } from "@/components/onboarding/section-frame";
import { PointsManager } from "@/components/onboarding/points-manager";

const tile = onboardingTile("points");
export const metadata: Metadata = { title: tile.title };

export default async function WelcomePointsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const members = readMembers();
  const lists = readLists();
  const meta = readMeta();
  const botOnline = isAvailable();

  return (
    <OnboardingSectionFrame
      guildId={guildId}
      title={tile.title}
      description="Edit a member's points, see who's where, and read the ranked lists. VIP floors come from the tier roles and never decay."
    >
      <PointsManager
        guildId={guildId}
        members={members}
        lists={lists}
        meta={meta}
        botOnline={botOnline}
      />
    </OnboardingSectionFrame>
  );
}
