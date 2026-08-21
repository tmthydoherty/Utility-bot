"use client";

import { useTransition } from "react";
import { useRouter } from "next/navigation";
import { useToast } from "@/components/ui/toast";
import { submitRoleShuffle } from "@/app/actions/roles-and-alerts";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { FormRolePicker } from "../components/form-picker";

export function ShuffleForm({ guildId }: { guildId: string }) {
  const router = useRouter();
  const toast = useToast();
  const [isPending, startTransition] = useTransition();

  async function action(formData: FormData) {
    startTransition(async () => {
      try {
        const triggerStr = formData.get("trigger_roles") as string;
        const gainRole = formData.get("gain_role") as string;
        const loseRole = formData.get("lose_role") as string;
        
        const triggerRoles = triggerStr ? triggerStr.split(",") : [];
        if (!triggerRoles.length) throw new Error("Must select at least one trigger role");
        if (!gainRole) throw new Error("Must select a gain role");
        
        await submitRoleShuffle(guildId, triggerRoles, gainRole, loseRole || null);
        toast.success("Role shuffle queued");
        router.refresh();
      } catch (err) {
        toast.error(err instanceof Error ? err.message : "Failed to queue shuffle");
      }
    });
  }

  return (
    <form action={action} className="space-y-6">
      <Card className="p-6">
          <div className="space-y-6">
            <div className="space-y-3">
              <label className="text-sm font-medium">Trigger Roles</label>
              <p className="text-sm text-fg-muted">Members must have ALL of these roles to be affected.</p>
              <FormRolePicker name="trigger_roles" defaultValue={[]} />
            </div>

            <div className="space-y-3">
              <label className="text-sm font-medium">Gain Role</label>
              <p className="text-sm text-fg-muted">Role to give to matching members.</p>
              <FormRolePicker name="gain_role" defaultValue={[]} max={1} />
            </div>

            <div className="space-y-3">
              <label className="text-sm font-medium">Lose Role (Optional)</label>
              <p className="text-sm text-fg-muted">Role to remove from matching members.</p>
              <FormRolePicker name="lose_role" defaultValue={[]} max={1} />
            </div>
          </div>
      </Card>

      <Button type="submit" disabled={isPending} className="w-full">
        {isPending ? "Queuing..." : "Queue Shuffle"}
      </Button>
    </form>
  );
}
