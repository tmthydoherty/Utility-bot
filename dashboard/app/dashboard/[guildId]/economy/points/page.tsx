import type { Metadata } from "next";

import { economyTab } from "@/lib/economy/tabs";
import { EconomyTabView } from "@/components/economy/economy-tab-view";

export const metadata: Metadata = { title: "Points" };

export default async function EconomyPointsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  return <EconomyTabView guildId={guildId} tab={economyTab("points")} />;
}
