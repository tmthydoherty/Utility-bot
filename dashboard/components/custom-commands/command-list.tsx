"use client";

import * as React from "react";
import Link from "next/link";
import { Plus, Sparkles } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { EmptyState } from "@/components/ui/empty-state";
import { useToast } from "@/components/ui/toast";
import { moderateGif } from "@/app/actions/custom-commands";
import type { CustomCommand, GifCommand, ListEntry } from "@/lib/custom-commands/types";

const MATCH_LABEL: Record<CustomCommand["matchType"], string> = {
  exact: "Exact command",
  startswith: "Starts with",
  contains: "Contains",
};

export function CommandList({
  guildId,
  entries,
  stats,
}: {
  guildId: string;
  entries: ListEntry[];
  stats: { commands: number; gifs: number; uses: number; topUsed: { name: string; count: number }[] };
}) {
  const base = `/dashboard/${guildId}/custom-commands`;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <Button asChild size="sm">
          <Link href={`${base}/new`}>
            <Plus aria-hidden /> New command
          </Link>
        </Button>
        <div className="flex gap-2 text-sm text-fg-subtle">
          <Stat label="commands" value={stats.commands} />
          <Stat label="purchased" value={stats.gifs} />
          <Stat label="uses" value={stats.uses} />
        </div>
      </div>

      {stats.topUsed.length > 0 && (
        <Card className="space-y-2 p-4">
          <div className="text-sm font-medium">Most used</div>
          <div className="flex flex-wrap gap-2">
            {stats.topUsed.map((t) => (
              <Badge key={t.name} variant="neutral">
                !{t.name} · {t.count.toLocaleString()}
              </Badge>
            ))}
          </div>
        </Card>
      )}

      {entries.length === 0 ? (
        <EmptyState
          icon="TerminalSquare"
          title="No commands yet"
          description="Make a !command that posts text, a GIF or a full embed — with random responses, role and channel limits, cooldowns and placeholders."
        />
      ) : (
        <div className="space-y-3">
          {entries.map((entry) =>
            entry.kind === "custom" ? (
              <CustomRow key={entry.command.id} base={base} command={entry.command} />
            ) : (
              <GifRow key={`gif:${entry.gif.name}`} guildId={guildId} gif={entry.gif} />
            ),
          )}
        </div>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <span className="rounded-md bg-[var(--surface-hover)] px-2 py-1">
      <span className="font-medium text-fg">{value.toLocaleString()}</span> {label}
    </span>
  );
}

function CustomRow({ base, command }: { base: string; command: CustomCommand }) {
  const title = command.matchType === "exact" ? `!${command.name}` : command.name;
  const subtitle = [
    MATCH_LABEL[command.matchType],
    `${command.responses.length || (command.plainText ? 0 : 1)} response${command.responses.length === 1 ? "" : "s"}`,
    `used ${command.useCount.toLocaleString()}×`,
  ].join(" · ");
  return (
    <Link href={`${base}/${command.id}`} className="block">
      <Card className="flex items-center gap-3 p-4 transition-colors hover:bg-[var(--surface-hover)]">
        <div className="min-w-0 flex-1">
          <div className="truncate font-medium">{title}</div>
          <div className="truncate text-sm text-fg-subtle">{subtitle}</div>
        </div>
        <Badge variant={command.enabled ? "success" : "neutral"}>{command.enabled ? "On" : "Off"}</Badge>
      </Card>
    </Link>
  );
}

function GifRow({ guildId, gif }: { guildId: string; gif: GifCommand }) {
  const toast = useToast();
  const [busy, setBusy] = React.useState(false);
  const [disabled, setDisabled] = React.useState(gif.disabled);
  const [deleted, setDeleted] = React.useState(false);
  const [pending, setPending] = React.useState(gif.pending);

  if (deleted) return null;

  const run = async (action: "disable" | "enable" | "delete") => {
    if (action === "delete" && !confirm(`Delete the purchased GIF command !${gif.name}? This frees the owner's slot.`))
      return;
    setBusy(true);
    const result = await moderateGif(guildId, gif.name, action);
    setBusy(false);
    if (!result.ok) {
      toast.error("Couldn't do that", result.error);
      return;
    }
    // The cog applies the change within ~10s; reflect the intent immediately.
    setPending(action);
    if (action === "disable") setDisabled(true);
    if (action === "enable") setDisabled(false);
    if (action === "delete") setDeleted(true);
    toast.success(
      action === "delete" ? "Queued for removal" : disabled ? "Turning back on" : "Turning off",
      "Live within about 10 seconds.",
    );
  };

  return (
    <Card className="flex items-center gap-3 p-4">
      <Sparkles className="size-4 shrink-0 text-fg-subtle" aria-hidden />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="truncate font-medium">!{gif.name}</span>
          <Badge variant="accent">★ econ purchased</Badge>
          {pending && <Badge variant="warning">applying…</Badge>}
        </div>
        <div className="truncate text-sm text-fg-subtle">
          Bought by &lt;@{gif.ownerId}&gt; · posts a GIF
        </div>
      </div>
      <Badge variant={disabled ? "neutral" : "success"}>{disabled ? "Off" : "On"}</Badge>
      <Button
        variant="ghost"
        size="sm"
        disabled={busy}
        onClick={() => run(disabled ? "enable" : "disable")}
      >
        {disabled ? "Enable" : "Disable"}
      </Button>
      <Button variant="danger" size="sm" disabled={busy} onClick={() => run("delete")}>
        Delete
      </Button>
    </Card>
  );
}
