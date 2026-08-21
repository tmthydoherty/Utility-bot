"use client";

import { useTransition } from "react";
import { useRouter } from "next/navigation";
import { useToast } from "@/components/ui/toast";
import { saveRolesAndAlertsSettings } from "@/app/actions/roles-and-alerts";
import type { RolesAndAlertsConfig } from "@/lib/roles-and-alerts/store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card } from "@/components/ui/card";
import { FormChannelPicker } from "../components/form-picker";

export function RolesAndAlertsSettingsForm({
  guildId,
  initialConfig,
}: {
  guildId: string;
  initialConfig: RolesAndAlertsConfig;
}) {
  const router = useRouter();
  const toast = useToast();
  const [isPending, startTransition] = useTransition();

  async function action(formData: FormData) {
    startTransition(async () => {
      try {
        const channelId = formData.get("channel_id") as string;
        const alertsBanner = formData.get("alerts_banner") as string;
        const colorsBanner = formData.get("colors_banner") as string;
        
        await saveRolesAndAlertsSettings(guildId, {
          channel_id: channelId || null,
          banners: {
            alerts: alertsBanner || null,
            colors: colorsBanner || null,
          },
        });
        toast.success("Settings saved");
        router.refresh();
      } catch (err) {
        toast.error(err instanceof Error ? err.message : "Failed to save settings");
      }
    });
  }

  return (
    <form action={action} className="space-y-6">
      <Card className="p-6">
        <div className="space-y-6">
          <div className="space-y-3">
            <label className="text-sm font-medium">Channel</label>
            <p className="text-sm text-fg-muted">The channel where the self-serve roles will be posted.</p>
            <FormChannelPicker
              name="channel_id"
              defaultValue={initialConfig.channel_id ? [initialConfig.channel_id] : []}
            />
          </div>

          <div className="space-y-3">
            <label className="text-sm font-medium">Alerts Banner URL</label>
            <p className="text-sm text-fg-muted">Optional image banner to display above the Alerts section.</p>
            <Input name="alerts_banner" defaultValue={initialConfig.banners?.alerts || ""} placeholder="https://..." />
          </div>

          <div className="space-y-3">
            <label className="text-sm font-medium">Colors Banner URL</label>
            <p className="text-sm text-fg-muted">Optional image banner to display above the Colors section.</p>
            <Input name="colors_banner" defaultValue={initialConfig.banners?.colors || ""} placeholder="https://..." />
          </div>
        </div>
      </Card>

      <Button type="submit" disabled={isPending}>
        {isPending ? "Saving..." : "Save settings"}
      </Button>
    </form>
  );
}
