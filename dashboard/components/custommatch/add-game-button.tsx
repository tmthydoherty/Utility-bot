"use client";

import * as React from "react";
import { Plus } from "lucide-react";

import { cn } from "@/lib/utils";
import { GAME_PRESETS, type GamePreset } from "@/lib/custommatch/presets";
import { addGame } from "@/app/actions/custommatch";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Sheet } from "@/components/ui/sheet";
import { useRun } from "./use-run";

/**
 * "New game" — pick a preset (or blank), name it, and create it. The preset fills
 * the four `add_game` arguments and any sensible extra columns; everything else
 * is set afterwards in the editor. Creation rides the bridge, so the new card
 * appears on the next reconcile.
 */
const DEFAULT_PRESET: GamePreset = GAME_PRESETS[0]!;

export function AddGameButton({ guildId }: { guildId: string }) {
  const { run, busy } = useRun();
  const [open, setOpen] = React.useState(false);
  const [preset, setPreset] = React.useState<GamePreset>(DEFAULT_PRESET);
  const [name, setName] = React.useState(DEFAULT_PRESET.name);
  const [players, setPlayers] = React.useState(String(DEFAULT_PRESET.playerCount));

  const choose = (p: GamePreset) => {
    setPreset(p);
    setName(p.name);
    setPlayers(String(p.playerCount));
  };

  const reset = () => {
    choose(DEFAULT_PRESET);
  };

  const playerCount = Number(players);
  const canCreate =
    name.trim().length > 0 && Number.isInteger(playerCount) && playerCount >= 2 && playerCount <= 40;

  const create = async () => {
    if (!canCreate) return;
    const ok = await run(
      () =>
        addGame(guildId, name.trim(), playerCount, preset.queueType, preset.captainSelection, preset.fields ?? {}),
      { success: "Game created" },
    );
    if (ok) {
      setOpen(false);
      reset();
    }
  };

  return (
    <>
      <Button onClick={() => setOpen(true)}>
        <Plus aria-hidden />
        New game
      </Button>

      <Sheet
        open={open}
        onOpenChange={setOpen}
        title="Add a game"
        description="Start from a preset, then fine-tune everything in the editor."
        footer={
          <div className="flex justify-end gap-2">
            <Button variant="ghost" onClick={() => setOpen(false)} disabled={busy}>
              Cancel
            </Button>
            <Button onClick={create} disabled={!canCreate || busy}>
              Create game
            </Button>
          </div>
        }
      >
        <div className="space-y-5">
          <div className="space-y-2">
            <span className="text-xs font-medium text-fg-muted">Preset</span>
            <div className="grid gap-2 sm:grid-cols-2">
              {GAME_PRESETS.map((p) => (
                <button
                  key={p.key}
                  type="button"
                  onClick={() => choose(p)}
                  className={cn(
                    "rounded-lg border p-3 text-left transition-colors",
                    p.key === preset.key
                      ? "border-[var(--accent)] bg-[var(--accent-soft)]"
                      : "border-[var(--border)] hover:bg-[var(--surface-hover)]",
                  )}
                >
                  <p className="text-sm font-medium">{p.label}</p>
                  <p className="mt-0.5 text-xs text-fg-subtle">{p.description}</p>
                </button>
              ))}
            </div>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-1.5">
              <span className="text-xs font-medium text-fg-muted">Name</span>
              <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Valorant" maxLength={100} disabled={busy} />
            </div>
            <div className="space-y-1.5">
              <span className="text-xs font-medium text-fg-muted">Players per match</span>
              <Input type="number" inputMode="numeric" value={players} onChange={(e) => setPlayers(e.target.value)} min={2} max={40} disabled={busy} />
            </div>
          </div>
        </div>
      </Sheet>
    </>
  );
}
