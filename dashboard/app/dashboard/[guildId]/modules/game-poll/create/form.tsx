"use client";

import * as React from "react";
import { useToast } from "@/components/ui/toast";
import { setDraftPoll } from "@/app/actions/game-poll";
import type { Game } from "@/lib/game-poll/store";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EntityPicker } from "@/components/ui/entity-picker";
import { Send, Clock } from "lucide-react";

export function CreatePollForm({ guildId, games, bannerUrl }: { guildId: string; games: Game[]; bannerUrl?: string }) {
  const toast = useToast();
  
  const [selectedIds, setSelectedIds] = React.useState<string[]>([]);
  const [endTime, setEndTime] = React.useState<string>("");
  const [gnTime, setGnTime] = React.useState<string>("");
  
  const [isSubmitting, setIsSubmitting] = React.useState(false);

  const selectedGames = selectedIds.map(id => games.find(g => g.id.toString() === id)).filter(Boolean) as Game[];

  const handlePost = async () => {
    if (selectedIds.length < 2 || selectedIds.length > 8) {
      toast.error("Please select between 2 and 8 games.");
      return;
    }
    if (!endTime) {
      toast.error("Poll end time is required.");
      return;
    }

    const end = new Date(endTime);
    if (isNaN(end.getTime()) || end.getTime() <= Date.now()) {
      toast.error("Poll end time must be in the future.");
      return;
    }

    let gnDate = null;
    if (gnTime) {
      gnDate = new Date(gnTime);
      if (isNaN(gnDate.getTime()) || gnDate.getTime() <= Date.now()) {
        toast.error("Game night time must be in the future.");
        return;
      }
    }

    setIsSubmitting(true);
    try {
      await setDraftPoll(
        selectedIds.map(Number),
        end,
        gnDate,
        guildId,
        true // postNow
      );
      toast.success("Poll created and triggered to post!");
      setSelectedIds([]);
      setEndTime("");
      setGnTime("");
    } catch {
      toast.error("Failed to post poll.");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="grid gap-6 lg:grid-cols-2">
      <div className="space-y-6">
        <Card className="p-6 space-y-6">
          <div className="space-y-2">
            <label className="text-sm font-medium">Select Games (2 to 8)</label>
            <EntityPicker
              multiple
              items={games.map(g => ({ value: g.id.toString(), label: g.name }))}
              value={selectedIds}
              onChange={(vals) => setSelectedIds(vals as string[])}
              placeholder="Search games..."
            />
          </div>

          <div className="space-y-2">
            <label className="text-sm font-medium">Poll End Time</label>
            <input 
              type="datetime-local" 
              value={endTime}
              onChange={(e) => setEndTime(e.target.value)}
              className="flex h-10 w-full rounded-md border border-[var(--border)] bg-bg-surface px-3 py-2 text-sm placeholder:text-fg-subtle focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)] focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
            />
            <p className="text-xs text-fg-muted">When should voting automatically close?</p>
          </div>

          <div className="space-y-2">
            <label className="text-sm font-medium">Game Night Time (Optional)</label>
            <input 
              type="datetime-local" 
              value={gnTime}
              onChange={(e) => setGnTime(e.target.value)}
              className="flex h-10 w-full rounded-md border border-[var(--border)] bg-bg-surface px-3 py-2 text-sm placeholder:text-fg-subtle focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)] focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
            />
            <p className="text-xs text-fg-muted">When does the event actually start?</p>
          </div>

          <Button onClick={handlePost} disabled={isSubmitting} className="w-full">
            <Send className="mr-2 size-4" />
            {isSubmitting ? "Posting..." : "Create & Post Poll"}
          </Button>
        </Card>
      </div>

      <div>
        <h3 className="mb-4 text-sm font-medium text-fg-muted uppercase tracking-wider">Preview</h3>
        <Card className="overflow-hidden border-[var(--border)] bg-[#313338] text-[#dbdee1] p-4 font-sans">
          <div className="flex gap-4">
            <div className="flex-1 space-y-4">
              <div className="flex items-center gap-2">
                <span className="font-semibold text-white">Vibey Bot</span>
                <span className="rounded bg-[#5865F2] px-1 text-[10px] font-bold text-white uppercase">Bot</span>
                <span className="text-xs text-[#949ba4]">Today at 12:00 PM</span>
              </div>

              <div className="border-l-4 border-[#2b2d31] bg-[#2b2d31] rounded-r-md p-4 space-y-4 shadow-sm max-w-lg">
                <div className="font-bold text-white text-base">Next Game Night Poll</div>
                <div className="text-sm leading-relaxed whitespace-pre-wrap">
                  It&apos;s time to choose our next game!
                  <br/><br/>
                  {selectedGames.length > 0 ? selectedGames.map(g => `**${g.name}**`).join("\n") : "*No games selected*"}
                  <br/><br/>
                  **Weighted Voting:**<br/>
                  Your 1st, 2nd, and 3rd choices carry different point values. Returning players from last week get a **2x multiplier** on their 1st place vote!
                </div>
                
                {endTime && (
                  <div className="mt-2">
                    <div className="text-xs font-semibold text-[#b5bac1]">Poll closes:</div>
                    <div className="text-sm bg-[#404249] text-white px-1.5 py-0.5 rounded inline-flex items-center gap-1 mt-1">
                      <Clock className="size-3" />
                      {new Date(endTime).toLocaleString()}
                    </div>
                  </div>
                )}
                
                {bannerUrl && (
                  <div className="mt-4">
                    {/* Preview of an admin-pasted arbitrary banner URL — next/image
                        would need every possible host whitelisted, so a plain img is
                        correct here. */}
                    {/* eslint-disable-next-line @next/next/no-img-element */}
                    <img src={bannerUrl} alt="Banner" className="rounded w-full object-cover max-h-64" />
                  </div>
                )}
              </div>

              <div className="flex gap-2 flex-wrap">
                <button className="bg-[#248046] hover:bg-[#1a6334] text-white font-medium text-sm px-4 py-2 rounded transition-colors" disabled>
                  Vote Now
                </button>
                <button className="bg-[#4e5058] hover:bg-[#6d6f78] text-white font-medium text-sm px-4 py-2 rounded transition-colors" disabled>
                  Game Night Role
                </button>
              </div>
            </div>
          </div>
        </Card>
      </div>
    </div>
  );
}
