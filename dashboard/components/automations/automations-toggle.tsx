"use client";

import * as React from "react";
import { useRouter } from "next/navigation";

import { setPausedAll } from "@/app/actions/automations";
import { StatusDot } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { useToast } from "@/components/ui/toast";

/**
 * The enable/disable switch for the whole Automations module, at the top of its
 * own page — the same shape as every other module's toggle.
 *
 * "Enabled" here means "not paused": under the hood it flips the bot's master
 * pause switch, so turning the module off stops every automation at once,
 * reversibly, without touching how any of them are configured. Optimistic — the
 * switch moves now and rolls back if the server disagrees.
 */
export function AutomationsToggle({
  guildId,
  initialEnabled,
}: {
  guildId: string;
  initialEnabled: boolean;
}) {
  const router = useRouter();
  const toast = useToast();
  const [enabled, setEnabled] = React.useState(initialEnabled);

  const onChange = async (next: boolean) => {
    setEnabled(next);
    const result = await setPausedAll(guildId, !next);
    if (!result.ok) {
      setEnabled(!next);
      toast.error("Couldn't change that", result.error);
      return;
    }
    router.refresh();
  };

  return (
    <Card className="flex items-center gap-3 p-4 sm:p-5">
      <StatusDot active={enabled} label={enabled ? "Enabled" : "Disabled"} />
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium">{enabled ? "Enabled" : "Disabled"}</p>
        <p className="text-xs text-fg-subtle">
          {enabled
            ? "Automations are running for this server."
            : "Every automation is switched off — nothing will run until you turn this back on."}
        </p>
      </div>
      <Switch
        checked={enabled}
        onCheckedChange={(next) => void onChange(next)}
        aria-label={enabled ? "Disable Automations" : "Enable Automations"}
      />
    </Card>
  );
}
