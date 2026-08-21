"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Copy, Trash2 } from "lucide-react";

import { cloneGame, deleteGame } from "@/app/actions/custommatch";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useToast } from "@/components/ui/toast";
import { useRun } from "./use-run";

/**
 * The irreversible corner of the game editor: duplicate the whole config, or
 * delete the game. Duplicating rides the normal bridge command (a full-copy with
 * fresh ranks). Deleting is guarded by a typed confirmation and, on success,
 * leaves the now-defunct editor for the section landing.
 */
export function DangerZone({
  guildId,
  gameId,
  gameName,
}: {
  guildId: string;
  gameId: number;
  gameName: string;
}) {
  const { run, busy } = useRun();
  const router = useRouter();
  const toast = useToast();
  const [cloneName, setCloneName] = React.useState(`${gameName} (copy)`);
  const [deleting, setDeleting] = React.useState(false);

  const duplicate = async () => {
    const name = cloneName.trim();
    if (!name) return;
    await run(() => cloneGame(guildId, gameId, name), { success: "Game duplicated" });
  };

  const remove = async () => {
    if (
      !window.confirm(
        `Delete "${gameName}"? Its games, MMR and stats are removed and this can't be undone.`,
      )
    )
      return;
    setDeleting(true);
    try {
      const result = await deleteGame(guildId, gameId);
      if (result.ok) {
        toast.success("Game deleted", "Vibey applies this within about 10 seconds.");
        router.push(`/dashboard/${guildId}/custom-matches`);
      } else {
        toast.error("Couldn't delete that", result.error ?? "Try again.");
        setDeleting(false);
      }
    } catch {
      toast.error("Couldn't reach the server", "Nothing was changed.");
      setDeleting(false);
    }
  };

  return (
    <div className="space-y-5">
      <div className="glass rounded-xl border border-[var(--border)] p-5">
        <h3 className="flex items-center gap-2 text-sm font-semibold">
          <Copy className="size-4" aria-hidden />
          Duplicate this game
        </h3>
        <p className="mt-1 text-sm text-fg-muted">
          Copies every setting and the rank ladder under a new name. The copy starts with no posted
          queue or leaderboard of its own.
        </p>
        <div className="mt-3 flex flex-wrap items-end gap-3">
          <div className="min-w-0 flex-1 space-y-1.5">
            <span className="text-xs font-medium text-fg-muted">New name</span>
            <Input
              value={cloneName}
              onChange={(e) => setCloneName(e.target.value)}
              maxLength={100}
              disabled={busy}
            />
          </div>
          <Button variant="secondary" onClick={duplicate} disabled={busy || !cloneName.trim()}>
            <Copy aria-hidden />
            Duplicate
          </Button>
        </div>
      </div>

      <div className="glass rounded-xl border border-[var(--danger)]/25 p-5">
        <h3 className="flex items-center gap-2 text-sm font-semibold text-[var(--danger)]">
          <Trash2 className="size-4" aria-hidden />
          Delete this game
        </h3>
        <p className="mt-1 text-sm text-fg-muted">
          Removes the game and everything tied to it — stats, MMR, ranks, queues and match history.
          There is no undo.
        </p>
        <div className="mt-3">
          <Button variant="danger" onClick={remove} disabled={deleting}>
            <Trash2 aria-hidden />
            {deleting ? "Deleting…" : "Delete game"}
          </Button>
        </div>
      </div>
    </div>
  );
}
