"use client";

import { useTransition, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useToast } from "@/components/ui/toast";
import { removeRoleShuffle } from "@/app/actions/roles-and-alerts";
import type { RoleShuffleTask } from "@/lib/roles-and-alerts/store";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { formatRelative } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { useGuild } from "@/components/providers/guild-provider";

export function ShuffleList({ guildId, tasks }: { guildId: string, tasks: RoleShuffleTask[] }) {
  const router = useRouter();
  const toast = useToast();
  const { roles } = useGuild();
  const [isPending, startTransition] = useTransition();

  function getRoleName(id: string) {
    return roles.find(r => r.id === id)?.name || id;
  }

  // Auto-refresh the page every 5 seconds if there are running/pending tasks
  useEffect(() => {
    if (tasks.some(t => t.status === "pending" || t.status === "running")) {
      const interval = setInterval(() => {
        router.refresh();
      }, 5000);
      return () => clearInterval(interval);
    }
  }, [tasks, router]);

  async function removeTask(id: string) {
    startTransition(async () => {
      try {
        await removeRoleShuffle(guildId, id);
        toast.success("Task deleted");
      } catch {
        toast.error("Failed to delete task");
      }
    });
  }

  if (tasks.length === 0) {
    return (
      <Card className="flex h-32 items-center justify-center p-6 text-sm text-fg-muted">
        No recent role shuffle tasks.
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      {tasks.map((task) => (
        <Card key={task.id} className="p-4">
          <div className="flex items-start justify-between">
            <div className="space-y-3">
              <div className="flex items-center gap-3">
                <Badge
                  variant={
                    task.status === "complete" ? "success"
                    : task.status === "running" ? "accent"
                    : task.status === "error" ? "danger"
                    : "neutral"
                  }
                >
                  {task.status}
                </Badge>
                <span className="text-xs text-fg-muted" suppressHydrationWarning>
                  {formatRelative(new Date(task.created_at + "Z"))}
                </span>
              </div>
              
              <div className="space-y-1">
                <div className="text-sm font-medium">Triggered by:</div>
                <div className="flex flex-wrap gap-1">
                  {task.trigger_roles.map((r) => (
                    <Badge key={r} variant="neutral">
                      {getRoleName(r)}
                    </Badge>
                  ))}
                </div>
              </div>
              <div className="space-y-1">
                <div className="text-sm font-medium">Gain role:</div>
                <Badge variant="neutral">
                  {getRoleName(task.gain_role)}
                </Badge>
              </div>
              {task.lose_role && (
                <div className="space-y-1">
                  <div className="text-sm font-medium">Lose role:</div>
                  <Badge variant="neutral">
                    {getRoleName(task.lose_role)}
                  </Badge>
                </div>
              )}

              {task.logs && (
                <div className="mt-2 text-sm text-fg-muted whitespace-pre-wrap bg-bg-subtle p-2 rounded">
                  {task.logs}
                </div>
              )}
            </div>
            
            <Button
              variant="ghost"
              size="sm"
              onClick={() => removeTask(task.id)}
              disabled={isPending || task.status === "running"}
            >
              Delete
            </Button>
          </div>
        </Card>
      ))}
    </div>
  );
}
