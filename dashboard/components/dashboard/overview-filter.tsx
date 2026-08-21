"use client";

import Link from "next/link";
import { ChevronDown, Filter } from "lucide-react";

import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/icon";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import type { SegmentOption } from "./segmented-links";

/**
 * The overview's section switcher, collapsed to a single filter control.
 *
 * The six sections (Server, Member, Channel, …) are still navigations — each is
 * a distinct URL the server renders — but rather than laying every one out as a
 * tab strip, only the current section shows, behind a filter icon. Opening the
 * menu reveals the rest. It keeps the top of the overview quiet: one control
 * instead of a two-row grid of tabs, and the choices live one tap away.
 */
export function OverviewFilter({
  options,
  "aria-label": ariaLabel,
}: {
  options: SegmentOption[];
  "aria-label"?: string;
}) {
  const active = options.find((o) => o.active) ?? options[0];

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        aria-label={ariaLabel}
        className={cn(
          "glass group inline-flex h-11 items-center gap-2 rounded-lg px-3.5",
          "text-sm font-medium text-fg outline-none transition-colors",
          "data-[state=open]:bg-[var(--surface-active)]",
        )}
      >
        <Filter className="size-4 shrink-0 text-fg-muted" strokeWidth={1.75} aria-hidden />
        {active?.icon && <Icon name={active.icon} className="size-4 shrink-0" />}
        <span>{active?.label}</span>
        <ChevronDown
          className="size-4 shrink-0 text-fg-muted transition-transform group-data-[state=open]:rotate-180"
          strokeWidth={1.75}
          aria-hidden
        />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start">
        {options.map((option) => (
          <DropdownMenuItem
            key={option.href}
            asChild
            className={cn(option.active && "bg-[var(--surface-active)] text-fg")}
          >
            <Link href={option.href} scroll={false}>
              {option.icon && <Icon name={option.icon} className="size-4 shrink-0" />}
              {option.label}
            </Link>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
