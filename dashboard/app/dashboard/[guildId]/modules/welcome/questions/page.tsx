import type { Metadata } from "next";

import { isAvailable, readQuestions } from "@/lib/onboarding/store";
import { onboardingTile } from "@/lib/onboarding/tabs";
import { OnboardingSectionFrame } from "@/components/onboarding/section-frame";
import { QuestionsManager } from "@/components/onboarding/questions-manager";

const tile = onboardingTile("questions");
export const metadata: Metadata = { title: tile.title };

export default async function WelcomeQuestionsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const questions = readQuestions();
  const botOnline = isAvailable();

  return (
    <OnboardingSectionFrame
      guildId={guildId}
      title={tile.title}
      description="The questions asked in the Introduce Yourself form, in order. Discord modals show at most five, so the first five are what members see."
    >
      <QuestionsManager guildId={guildId} questions={questions} botOnline={botOnline} />
    </OnboardingSectionFrame>
  );
}
