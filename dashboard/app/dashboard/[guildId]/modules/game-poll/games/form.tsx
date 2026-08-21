"use client";

import * as React from "react";
import { Plus, Trash2, Pencil, Gamepad2 } from "lucide-react";
import { useToast } from "@/components/ui/toast";
import { saveGame, removeGame } from "@/app/actions/game-poll";
import type { Game } from "@/lib/game-poll/store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card } from "@/components/ui/card";

export function GamesForm({ guildId, initialGames }: { guildId: string; initialGames: Game[] }) {
  const toast = useToast();
  const [games, setGames] = React.useState(initialGames);
  const [editing, setEditing] = React.useState<Game | Partial<Game> | null>(null);

  // Sync state when props change
  React.useEffect(() => {
    setGames(initialGames);
  }, [initialGames]);

  async function handleSave(formData: FormData) {
    const name = formData.get("name") as string;
    const banner_url = formData.get("banner_url") as string;
    
    if (!name.trim()) return;

    try {
      await saveGame(
        {
          id: editing && 'id' in editing ? editing.id : undefined,
          name: name.trim(),
          banner_url: banner_url.trim() || null,
        },
        guildId
      );
      setEditing(null);
      toast.success("Game saved successfully");
    } catch {
      toast.error("Failed to save game");
    }
  }

  async function handleDelete(id: number) {
    if (!confirm("Are you sure you want to delete this game?")) return;
    try {
      await removeGame(id, guildId);
      toast.success("Game deleted");
    } catch {
      toast.error("Failed to delete game");
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-medium">Games Library</h2>
        <Button onClick={() => setEditing({ name: "", banner_url: "" })} size="sm">
          <Plus className="mr-2 size-4" />
          Add Game
        </Button>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {games.map((g) => (
          <Card key={g.id} className="group flex flex-col overflow-hidden">
            {g.banner_url ? (
              <div
                className="h-24 w-full bg-cover bg-center"
                style={{ backgroundImage: `url(${g.banner_url})` }}
              />
            ) : (
              <div className="flex h-24 w-full items-center justify-center bg-bg-surface-2 text-fg-muted">
                <Gamepad2 className="size-8 opacity-50" />
              </div>
            )}
            <div className="flex items-center gap-2 p-4">
              <h3 className="min-w-0 flex-1 truncate text-base font-semibold" title={g.name}>
                {g.name}
              </h3>
              <div className="flex shrink-0 items-center gap-0.5">
                <Button
                  variant="ghost"
                  size="icon"
                  className="size-8 text-fg-muted hover:text-fg"
                  onClick={() => setEditing(g)}
                  aria-label={`Edit ${g.name}`}
                >
                  <Pencil className="size-4" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  className="size-8 text-fg-muted hover:text-[var(--danger)]"
                  onClick={() => handleDelete(g.id)}
                  aria-label={`Delete ${g.name}`}
                >
                  <Trash2 className="size-4" />
                </Button>
              </div>
            </div>
          </Card>
        ))}
      </div>

      {editing !== null && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-sm p-4">
          <Card className="w-full max-w-md p-6 border-zinc-700 bg-zinc-900 shadow-xl">
            <h2 className="mb-4 text-lg font-semibold">
              {editing && 'id' in editing ? "Edit Game" : "Add Game"}
            </h2>
            <form action={handleSave} className="space-y-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Name</label>
                <Input
                  name="name"
                  defaultValue={editing?.name || ""}
                  placeholder="e.g. Valorant"
                  required
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Banner URL</label>
                <Input
                  name="banner_url"
                  defaultValue={editing?.banner_url || ""}
                  placeholder="https://example.com/image.png"
                />
                <p className="text-xs text-fg-muted">Optional cover image for the game</p>
              </div>
              <div className="mt-6 flex justify-end gap-2">
                <Button type="button" variant="secondary" onClick={() => setEditing(null)}>Cancel</Button>
                <Button type="submit">Save</Button>
              </div>
            </form>
          </Card>
        </div>
      )}
    </div>
  );
}
