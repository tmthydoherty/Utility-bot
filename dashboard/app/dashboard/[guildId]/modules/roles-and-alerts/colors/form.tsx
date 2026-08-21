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

export function ColorsForm({
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
        const t1Str = formData.get("colors_t1") as string;
        const t2Str = formData.get("colors_t2") as string;
        const t3Str = formData.get("colors_t3") as string;
        
        await saveRolesAndAlertsSettings(guildId, {
          colors: {
            t1: t1Str ? t1Str.split(",") : [],
            t2: t2Str ? t2Str.split(",") : [],
            t3: t3Str ? t3Str.split(",") : [],
          }
        });
        toast.success("Colors saved");
        setEditing(new Set());
        setDirty(false);
        router.refresh();
      } catch (err) {
        toast.error(err instanceof Error ? err.message : "Failed to save colors");
      }
    });
  }

  return (
    <form action={action} className="space-y-6" onChange={() => setDirty(true)}>
      <div className="grid gap-6 md:grid-cols-3">
        <SectionCard
          title="Tier 1 Colors"
          description="Available to all members."
          editing={editing.has("t1")}
          onToggle={() => toggleEdit("t1")}
        >
          <div className="p-5 pt-0 sm:p-6 sm:pt-0">
            <FormRolePicker
              name="colors_t1"
              defaultValue={initialConfig.colors.t1}
              disabled={!editing.has("t1") || isPending}
            />
          </div>
        </SectionCard>

        <SectionCard
          title="Tier 2 Colors"
          description="Requires Tier 2 Gate role."
          editing={editing.has("t2")}
          onToggle={() => toggleEdit("t2")}
        >
          <div className="p-5 pt-0 sm:p-6 sm:pt-0">
            <FormRolePicker
              name="colors_t2"
              defaultValue={initialConfig.colors.t2}
              disabled={!editing.has("t2") || isPending}
            />
          </div>
        </SectionCard>

        <SectionCard
          title="Tier 3 Colors"
          description="Requires Tier 3 Gate role."
          editing={editing.has("t3")}
          onToggle={() => toggleEdit("t3")}
        >
          <div className="p-5 pt-0 sm:p-6 sm:pt-0">
            <FormRolePicker
              name="colors_t3"
              defaultValue={initialConfig.colors.t3}
              disabled={!editing.has("t3") || isPending}
            />
          </div>
        </SectionCard>
      </div>

      <SaveBar
        visible={dirty}
        saving={isPending}
        error={null}
        changeCount={1}
        onSave={() => document.getElementById('colors-submit')?.click()}
        onReset={() => { setEditing(new Set()); setDirty(false); router.refresh(); }}
      />
      <button type="submit" id="colors-submit" className="hidden" disabled={isPending} />
    </form>
  );
}
