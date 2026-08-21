"use client";

import * as React from "react";

import { toggleModule } from "@/app/actions/settings";
import { StatusDot } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { useToast } from "@/components/ui/toast";

/**
 * The enable/disable switch for a module, on the module's own page.
 *
 * It used to sit on the grid tile; moved here so the change is made next to the
 * settings it governs. Optimistic — the switch moves now and rolls back if the
 * server disagrees, because a toggle that waits on a round trip feels broken.
 */
export function ModuleToggle({
  guildId,
  moduleId,
  moduleName,
  initialEnabled,
}: {
  guildId: string;
  moduleId: string;
  moduleName: string;
  initialEnabled: boolean;
}) {
  const toast = useToast();
  const [enabled, setEnabled] = React.useState(initialEnabled);

  const onChange = async (next: boolean) => {
    setEnabled(next);
    const result = await toggleModule(guildId, moduleId, next);
    if (!result.ok) {
      setEnabled(!next);
      toast.error("Couldn't change that", result.error);
    }
  };

  return (
    <Card className="flex items-center gap-3 p-4 sm:p-5">
      <StatusDot active={enabled} label={enabled ? "Enabled" : "Disabled"} />
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium">{enabled ? "Enabled" : "Disabled"}</p>
        <p className="text-xs text-fg-subtle">
          {enabled
            ? `${moduleName} is on for this server.`
            : `${moduleName} is switched off for this server.`}
        </p>
      </div>
      <Switch
        checked={enabled}
        onCheckedChange={(next) => void onChange(next)}
        aria-label={`${enabled ? "Disable" : "Enable"} ${moduleName}`}
      />
    </Card>
  );
}
