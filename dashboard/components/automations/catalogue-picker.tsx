"use client";

import * as React from "react";
import { Search } from "lucide-react";

import { cn } from "@/lib/utils";
import { Icon } from "@/components/ui/icon";
import { Input } from "@/components/ui/input";
import { Sheet } from "@/components/ui/sheet";

/**
 * "Pick a thing from the catalogue", used for all three catalogues.
 *
 * Every entry shows its heading, what it means in plain words, and — where one
 * exists — a concrete example. That is three lines per option instead of one,
 * and it is worth every pixel: the Discord panel can only afford the heading,
 * which is exactly why "The message contains certain words" and "The message
 * is exactly something" are indistinguishable there until you have picked the
 * wrong one twice.
 *
 * Search matches all three, so someone who does not know the vocabulary can
 * still type "shouting" and find the caps check.
 */

export interface CatalogueEntry {
  key: string;
  label: string;
  plain: string;
  example?: string;
  icon: string;
  category: string;
  destructive?: boolean;
}

export function CataloguePicker({
  open,
  onOpenChange,
  title,
  description,
  entries,
  selected,
  onPick,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description?: string;
  entries: CatalogueEntry[];
  selected?: string;
  onPick: (key: string) => void;
}) {
  const [query, setQuery] = React.useState("");

  // Cleared on open rather than on close, so the list is never seen resetting
  // as the sheet animates away.
  React.useEffect(() => {
    if (open) setQuery("");
  }, [open]);

  const groups = React.useMemo(() => {
    const needle = query.trim().toLowerCase();
    const matched = needle
      ? entries.filter((entry) =>
          `${entry.label} ${entry.plain} ${entry.example ?? ""} ${entry.category}`
            .toLowerCase()
            .includes(needle),
        )
      : entries;

    const grouped = new Map<string, CatalogueEntry[]>();
    for (const entry of matched) {
      const existing = grouped.get(entry.category);
      if (existing) existing.push(entry);
      else grouped.set(entry.category, [entry]);
    }
    return [...grouped.entries()];
  }, [entries, query]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange} title={title} description={description} size="lg">
      <div className="space-y-4">
        <div className="relative">
          <Search
            className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-fg-subtle"
            aria-hidden
          />
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search…"
            className="pl-9"
            aria-label={`Search ${title.toLowerCase()}`}
            autoFocus
          />
        </div>

        {groups.length === 0 ? (
          <p className="py-8 text-center text-sm text-fg-muted">
            Nothing matches “{query}”.
          </p>
        ) : (
          <div className="space-y-5">
            {groups.map(([category, items]) => (
              <section key={category} className="space-y-1.5">
                <h3 className="px-1 text-xs font-semibold uppercase tracking-wide text-fg-subtle">
                  {category}
                </h3>
                <ul className="space-y-1.5">
                  {items.map((entry) => (
                    <li key={entry.key}>
                      <button
                        type="button"
                        onClick={() => {
                          onPick(entry.key);
                          onOpenChange(false);
                        }}
                        className={cn(
                          "flex w-full items-start gap-3 rounded-lg border p-3 text-left transition-colors",
                          entry.key === selected
                            ? "border-[var(--accent)] bg-[var(--accent-soft)]"
                            : "border-[var(--border)] hover:border-[var(--border-strong)] hover:bg-[var(--surface-hover)]",
                        )}
                      >
                        <span
                          className={cn(
                            "grid size-8 shrink-0 place-items-center rounded-md",
                            entry.destructive
                              ? "bg-[var(--danger-soft)] text-[var(--danger)]"
                              : "bg-[var(--surface-active)] text-fg-muted",
                          )}
                          aria-hidden
                        >
                          <Icon name={entry.icon} className="size-4" />
                        </span>
                        <span className="min-w-0 flex-1">
                          <span className="flex flex-wrap items-center gap-2">
                            <span className="text-sm font-medium">{entry.label}</span>
                            {entry.destructive && (
                              <span className="rounded-full bg-[var(--danger-soft)] px-1.5 py-0.5 text-[10px] font-medium text-[var(--danger)]">
                                takes something away
                              </span>
                            )}
                          </span>
                          <span className="mt-0.5 block text-xs leading-relaxed text-fg-muted">
                            {entry.plain}
                          </span>
                          {entry.example && (
                            <span className="mt-1 block text-xs italic leading-relaxed text-fg-subtle">
                              {entry.example}
                            </span>
                          )}
                        </span>
                      </button>
                    </li>
                  ))}
                </ul>
              </section>
            ))}
          </div>
        )}
      </div>
    </Sheet>
  );
}
