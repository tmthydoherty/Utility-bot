import { redirect } from "next/navigation";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { readSettings } from "@/lib/bot/adapter";
import { Button } from "@/components/ui/button";
import { gamePoll } from "@/lib/schema/modules";
import { ModuleSettingsForm } from "@/components/settings/module-settings-form";

export default async function GamePollSettingsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const mod = gamePoll;
  if (!mod) redirect(`/dashboard/${guildId}`);

  const values = readSettings(guildId, "game-poll");

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules/game-poll`}>
          <ArrowLeft aria-hidden />
          Game Polls
        </Link>
      </Button>
      <ModuleSettingsForm
        module={mod}
        guildId={guildId}
        initialValues={values}
      />
    </div>
  );
}
