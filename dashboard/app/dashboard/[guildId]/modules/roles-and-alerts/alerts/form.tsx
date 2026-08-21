"use client";

import { useTransition } from "react";
import { useRouter } from "next/navigation";
import { useToast } from "@/components/ui/toast";
import { saveRolesAndAlertsSettings } from "@/app/actions/roles-and-alerts";
import type { RolesAndAlertsConfig } from "@/lib/roles-and-alerts/store";
import { FormRolePicker } from "../components/form-picker";
import { SectionCard } from "../components/section-card";
import { useState } from "react";
import { SaveBar } from "@/components/ui/save-bar";

export function AlertsForm({
  guildId,
  initialConfig,
}: {
  guildId: string;
  initialConfig: RolesAndAlertsConfig;
}) {
  const router = useRouter();
  const toast = useToast();
  const [isPending, startTransition] = useTransition();
  const [editing, setEditing] = useState(false);
  const [dirty, setDirty] = useState(false);

  async function action(formData: FormData) {
    startTransition(async () => {
      try {
        const alertsStr = formData.get("alert_roles") as string;
        
        await saveRolesAndAlertsSettings(guildId, {
          alert_roles: alertsStr ? alertsStr.split(",") : [],
        });
        toast.success("Alert roles saved");
        setEditing(false);
        setDirty(false);
        router.refresh();
      } catch (err) {
        toast.error(err instanceof Error ? err.message : "Failed to save alert roles");
      }
    });
  }

  return (
    <form action={action} className="space-y-6" onChange={() => setDirty(true)}>
      <SectionCard
        title="Alert Roles"
        description="Roles that members can add/remove to get pinged."
        editing={editing}
        onToggle={() => setEditing(!editing)}
      >
        <div className="p-5 pt-0 sm:p-6 sm:pt-0">
          <FormRolePicker
            name="alert_roles"
            defaultValue={initialConfig.alert_roles}
            disabled={!editing || isPending}
          />
        </div>
      </SectionCard>

      <SaveBar
        visible={dirty}
        saving={isPending}
        error={null}
        changeCount={1}
        onSave={() => document.getElementById('alerts-submit')?.click()}
        onReset={() => { setEditing(false); setDirty(false); router.refresh(); }}
      />
      <button type="submit" id="alerts-submit" className="hidden" disabled={isPending} />
    </form>
  );
}
