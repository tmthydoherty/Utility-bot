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

export function TiersForm({
  guildId,
  initialConfig,
}: {
  guildId: string;
  initialConfig: RolesAndAlertsConfig;
}) {
  const router = useRouter();
  const toast = useToast();
  const [isPending, startTransition] = useTransition();
  const [editing, setEditing] = useState<Set<string>>(new Set());
  const [dirty, setDirty] = useState(false);

  const toggleEdit = (id: string) => {
    setEditing(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  async function action(formData: FormData) {
    startTransition(async () => {
      try {
        const t2Gate = formData.get("gate_t2") as string;
        const t3Gate = formData.get("gate_t3") as string;
        
        const t2VipStr = formData.get("vip_t2") as string;
        const t3VipStr = formData.get("vip_t3") as string;
        
        const t2Vip = t2VipStr ? t2VipStr.split(",") : [];
        const t3Vip = t3VipStr ? t3VipStr.split(",") : [];
        
        await saveRolesAndAlertsSettings(guildId, {
          gates: {
            t2: t2Gate || null,
            t3: t3Gate || null,
          },
          vip_roles: {
            t2: t2Vip,
            t3: t3Vip,
          }
        });
        toast.success("Tiers saved");
        setEditing(new Set());
        setDirty(false);
        router.refresh();
      } catch (err) {
        toast.error(err instanceof Error ? err.message : "Failed to save tiers");
      }
    });
  }

  return (
    <form action={action} className="space-y-6" onChange={() => setDirty(true)}>
      <div className="grid gap-6 md:grid-cols-2">
        {/* Tier 2 */}
        <SectionCard
          title="Tier 2"
          description="Configuration for Tier 2 access and VIP roles."
          editing={editing.has("t2")}
          onToggle={() => toggleEdit("t2")}
        >
          <div className="p-5 pt-0 sm:p-6 sm:pt-0 space-y-6 mt-4">
            <div className="space-y-3">
              <label className="text-sm font-medium">Gate Role</label>
              <p className="text-sm text-fg-muted">The role required to access Tier 2 colors.</p>
              <FormRolePicker
                name="gate_t2"
                defaultValue={initialConfig.gates.t2 ? [initialConfig.gates.t2] : []}
                max={1}
                disabled={!editing.has("t2") || isPending}
              />
            </div>
            <div className="space-y-3">
              <label className="text-sm font-medium">VIP Roles</label>
              <p className="text-sm text-fg-muted">Roles that permanently grant Tier 2 points floor.</p>
              <FormRolePicker
                name="vip_t2"
                defaultValue={initialConfig.vip_roles.t2}
                disabled={!editing.has("t2") || isPending}
              />
            </div>
          </div>
        </SectionCard>

        {/* Tier 3 */}
        <SectionCard
          title="Tier 3"
          description="Configuration for Tier 3 access and VIP roles."
          editing={editing.has("t3")}
          onToggle={() => toggleEdit("t3")}
        >
          <div className="p-5 pt-0 sm:p-6 sm:pt-0 space-y-6 mt-4">
            <div className="space-y-3">
              <label className="text-sm font-medium">Gate Role</label>
              <p className="text-sm text-fg-muted">The role required to access Tier 3 colors.</p>
              <FormRolePicker
                name="gate_t3"
                defaultValue={initialConfig.gates.t3 ? [initialConfig.gates.t3] : []}
                max={1}
                disabled={!editing.has("t3") || isPending}
              />
            </div>
            <div className="space-y-3">
              <label className="text-sm font-medium">VIP Roles</label>
              <p className="text-sm text-fg-muted">Roles that permanently grant Tier 3 points floor.</p>
              <FormRolePicker
                name="vip_t3"
                defaultValue={initialConfig.vip_roles.t3}
                disabled={!editing.has("t3") || isPending}
              />
            </div>
          </div>
        </SectionCard>
      </div>

      <SaveBar
        visible={dirty}
        saving={isPending}
        error={null}
        changeCount={1}
        onSave={() => document.getElementById('tiers-submit')?.click()}
        onReset={() => { setEditing(new Set()); setDirty(false); router.refresh(); }}
      />
      <button type="submit" id="tiers-submit" className="hidden" disabled={isPending} />
    </form>
  );
}
