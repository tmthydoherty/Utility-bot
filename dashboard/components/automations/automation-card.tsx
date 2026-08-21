"use client";

import * as React from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { ChevronRight, Copy, MoreVertical, Trash2, Wrench } from "lucide-react";

import { cn, formatRelative } from "@/lib/utils";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useToast } from "@/components/ui/toast";
import { TRIGGERS } from "@/lib/automations/registry";
import { stateOf, type Automation } from "@/lib/automations/types";
import * as actions from "@/app/actions/automations";

import { AutomationSentence } from "./sentence";
import { StatePill, StateControl } from "./state-control";

/**
 * One automation, as a card.
 *
 * The sentence is the headline and the name is the small print, which is the
 * opposite of the obvious layout and the right way round: people name these
 * things "test 2" and then cannot tell them apart a week later. What the
 * automation *does* is the only reliable way to recognise it.
 */
export function AutomationCard({
  automation,
  guildId,
  unfinished,
}: {
  automation: Automation;
  guildId: string;
  /** From `readinessSummary` on the server — "2 things still to fill in". */
  unfinished: string;
}) {
  const router = useRouter();
  const { toast, error: toastError } = useToast();
  const [pending, setPending] = React.useState(false);
  const [confirmingDelete, setConfirmingDelete] = React.useState(false);

  const trigger = TRIGGERS[automation.triggerType];
  const state = stateOf(automation);
  const href = `/dashboard/${guildId}/automations/${automation.id}`;

  const changeState = async (next: typeof state) => {
    setPending(true);
    const result = await actions.setState(guildId, automation.id, next);
    setPending(false);
    if (!result.ok) {
      toastError("Couldn't change that", result.error);
      return;
    }
    // Ten seconds is the bot's polling interval, and saying so is better than
    // leaving someone wondering whether it took.
    toast({
      variant: "success",
      title:
        next === "off"
          ? "Switched off"
          : next === "testing"
            ? "Now in test mode"
            : "Switched on",
      description: next === "off" ? undefined : "Vibey picks this up within about 10 seconds.",
    });
    router.refresh();
  };

  const run = async (label: string, fn: () => Promise<actions.ActionResult>) => {
    setPending(true);
    const result = await fn();
    setPending(false);
    if (!result.ok) {
      toastError(`Couldn't ${label}`, result.error);
      return;
    }
    router.refresh();
    return result;
  };

  return (
    <Card
      className={cn(
        "group relative flex flex-col gap-4 p-5 transition-colors",
        "hover:border-[var(--border-strong)]",
        pending && "opacity-60",
      )}
    >
      <div className="flex items-start gap-3">
        <span
          className={cn(
            "grid size-9 shrink-0 place-items-center rounded-lg",
            "bg-[var(--accent-soft)] text-[var(--accent)]",
          )}
          aria-hidden
        >
          <Icon name={trigger?.icon ?? "Zap"} className="size-[18px]" />
        </span>

        <div className="min-w-0 flex-1">
          {/* The whole card is the link target rather than just the title —
              a 3px text row is a poor tap target on a phone. */}
          <Link href={href} className="after:absolute after:inset-0" aria-label={`Edit ${automation.name}`}>
            <h3 className="truncate text-sm font-semibold">{automation.name}</h3>
          </Link>
          <p className="mt-0.5 truncate text-xs text-fg-subtle">
            {automation.runCount > 0
              ? `Ran ${automation.runCount.toLocaleString()} time${automation.runCount === 1 ? "" : "s"}`
              : "Hasn't run yet"}
            {automation.lastFiredAt > 0 && ` · last ${formatRelative(automation.lastFiredAt * 1000)}`}
          </p>
        </div>

        <StatePill state={state} />
      </div>

      <AutomationSentence automation={automation} size="sm" className="text-fg-muted" />

      {unfinished && (
        <p className="flex items-center gap-1.5 text-xs font-medium text-[var(--warning)]">
          <Wrench className="size-3.5 shrink-0" aria-hidden />
          {unfinished}
        </p>
      )}

      {/* Above the card link, so these stay clickable. */}
      <div className="relative z-10 flex items-center justify-between gap-3 border-t border-[var(--border)] pt-4">
        <StateControl
          value={state}
          onChange={changeState}
          disabled={pending}
          blockedReason={unfinished ? "Finish it first" : undefined}
          size="sm"
          className="[&>p]:hidden"
        />

        <div className="flex items-center gap-1">
          <DropdownMenu modal={false}>
            <DropdownMenuTrigger asChild>
              <button
                type="button"
                className="grid size-8 place-items-center rounded-md text-fg-subtle transition-colors hover:bg-[var(--surface-hover)] hover:text-fg"
                aria-label={`More options for ${automation.name}`}
              >
                <MoreVertical className="size-4" aria-hidden />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent>
              <DropdownMenuItem
                onSelect={() =>
                  run("make a copy", () => actions.duplicate(guildId, automation.id))
                }
              >
                <Copy aria-hidden />
                Make a copy
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem
                destructive
                onSelect={(event) => {
                  // Two taps, not a dialog: a confirm dialog for something
                  // this small is a speed bump people learn to click through.
                  event.preventDefault();
                  if (!confirmingDelete) {
                    setConfirmingDelete(true);
                    setTimeout(() => setConfirmingDelete(false), 4000);
                    return;
                  }
                  void run("delete it", () => actions.remove(guildId, automation.id));
                }}
              >
                <Trash2 aria-hidden />
                {confirmingDelete ? "Tap again to delete" : "Delete"}
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>

          <Link
            href={href}
            className="grid size-8 place-items-center rounded-md text-fg-subtle transition-colors hover:bg-[var(--surface-hover)] hover:text-fg"
            aria-label={`Edit ${automation.name}`}
          >
            <ChevronRight className="size-4" aria-hidden />
          </Link>
        </div>
      </div>
    </Card>
  );
}
