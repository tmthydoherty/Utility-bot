"use client";

import Link from "next/link";
import { CalendarRange } from "lucide-react";

import { cn } from "@/lib/utils";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

export interface TimeframeOption {
  label: string;
  href: string;
  active: boolean;
}

/**
 * The tracker's universal date-range control — one icon-button dropdown used by
 * every section of the page (overview, channel, emoji, leaderboard, compare).
 *
 * Collapsed it's just the calendar icon, in the same muted tone as the section
 * filter's funnel; the current timeframe and the alternatives only appear once
 * the menu is open. Each range is a navigation — a URL the server re-renders —
 * so choosing one updates the whole section.
 */
export function TimeframeMenu({
  options,
  "aria-label": ariaLabel = "Date range",
}: {
  options: TimeframeOption[];
  "aria-label"?: string;
}) {
  const active = options.find((o) => o.active) ?? options[0];

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        aria-label={active ? `${ariaLabel}: ${active.label}` : ariaLabel}
        className={cn(
          "glass grid size-11 shrink-0 place-items-center rounded-lg",
          "text-fg-muted outline-none transition-colors",
          "hover:text-fg data-[state=open]:bg-[var(--surface-active)] data-[state=open]:text-fg",
        )}
      >
        <CalendarRange className="size-4" strokeWidth={1.75} aria-hidden />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuLabel>Date range</DropdownMenuLabel>
        {options.map((option) => (
          <DropdownMenuItem
            key={option.href}
            asChild
            className={cn(option.active && "bg-[var(--surface-active)] text-fg")}
          >
            <Link href={option.href} scroll={false}>
              {option.label}
            </Link>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
