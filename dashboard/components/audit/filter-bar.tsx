"use client";

import Link from "next/link";
import { ChevronDown, Download, Filter, User, X } from "lucide-react";

import { cn } from "@/lib/utils";
import { describeCategory } from "@/lib/audit/labels";
import { buttonVariants } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

/**
 * The audit log's filter + export controls.
 *
 * Every filter lives in the URL, so a filtered view is a real, shareable,
 * back-button-able page the server renders — the dropdowns are just links that
 * rewrite the query and drop the page back to the first one. Export points at
 * the CSV route carrying the same query, so "export" always means "export what
 * this filter shows".
 */
export function AuditFilterBar({
  basePath,
  exportPath,
  actors,
  categories,
  activeActor,
  activeCategory,
}: {
  basePath: string;
  exportPath: string;
  actors: { actorId: string; actorName: string }[];
  categories: string[];
  activeActor: string | null;
  activeCategory: string | null;
}) {
  const hrefWith = (patch: Record<string, string | null>) => {
    const params = new URLSearchParams();
    const actor = "actor" in patch ? patch.actor : activeActor;
    const category = "category" in patch ? patch.category : activeCategory;
    if (actor) params.set("actor", actor);
    if (category) params.set("category", category);
    // Any filter change returns to page one — page 3 of the old filter is
    // meaningless under the new one.
    const qs = params.toString();
    return qs ? `${basePath}?${qs}` : basePath;
  };

  const activeActorName = actors.find((a) => a.actorId === activeActor)?.actorName;
  const hasFilter = Boolean(activeActor || activeCategory);

  const exportHref = (() => {
    const params = new URLSearchParams();
    if (activeActor) params.set("actor", activeActor);
    if (activeCategory) params.set("category", activeCategory);
    const qs = params.toString();
    return qs ? `${exportPath}?${qs}` : exportPath;
  })();

  return (
    <div className="flex flex-wrap items-center gap-2">
      {/* Who */}
      {actors.length > 0 && (
        <DropdownMenu>
          <FilterTrigger
            icon={<User className="size-4 shrink-0 text-fg-muted" aria-hidden />}
            label={activeActorName ?? "Anyone"}
            active={Boolean(activeActor)}
          />
          <DropdownMenuContent align="start" className="max-h-72 overflow-y-auto">
            <DropdownMenuItem asChild>
              <Link href={hrefWith({ actor: null })}>Anyone</Link>
            </DropdownMenuItem>
            {actors.map((a) => (
              <DropdownMenuItem
                key={a.actorId}
                asChild
                className={cn(a.actorId === activeActor && "bg-[var(--surface-active)] text-fg")}
              >
                <Link href={hrefWith({ actor: a.actorId })}>{a.actorName}</Link>
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      )}

      {/* What */}
      {categories.length > 0 && (
        <DropdownMenu>
          <FilterTrigger
            icon={<Filter className="size-4 shrink-0 text-fg-muted" aria-hidden />}
            label={activeCategory ? describeCategory(activeCategory) : "All areas"}
            active={Boolean(activeCategory)}
          />
          <DropdownMenuContent align="start" className="max-h-72 overflow-y-auto">
            <DropdownMenuItem asChild>
              <Link href={hrefWith({ category: null })}>All areas</Link>
            </DropdownMenuItem>
            {categories.map((c) => (
              <DropdownMenuItem
                key={c}
                asChild
                className={cn(c === activeCategory && "bg-[var(--surface-active)] text-fg")}
              >
                <Link href={hrefWith({ category: c })}>{describeCategory(c)}</Link>
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      )}

      {hasFilter && (
        <Link
          href={basePath}
          className="inline-flex h-9 items-center gap-1.5 rounded-lg px-2.5 text-sm text-fg-muted transition-colors hover:text-fg"
        >
          <X className="size-4" aria-hidden />
          Clear
        </Link>
      )}

      <a
        href={exportHref}
        className={cn(buttonVariants({ variant: "secondary", size: "sm" }), "ml-auto")}
        // A plain anchor, not <Link>: this is a file download, not a client
        // navigation, so the browser's own download handling must take over.
        download
      >
        <Download aria-hidden />
        <span className="hidden sm:inline">Export CSV</span>
      </a>
    </div>
  );
}

function FilterTrigger({
  icon,
  label,
  active,
}: {
  icon: React.ReactNode;
  label: string;
  active: boolean;
}) {
  return (
    <DropdownMenuTrigger
      className={cn(
        "glass group inline-flex h-9 items-center gap-2 rounded-lg px-3 text-sm font-medium outline-none transition-colors",
        "data-[state=open]:bg-[var(--surface-active)]",
        active ? "text-fg" : "text-fg-muted",
      )}
    >
      {icon}
      <span>{label}</span>
      <ChevronDown
        className="size-4 shrink-0 text-fg-muted transition-transform group-data-[state=open]:rotate-180"
        aria-hidden
      />
    </DropdownMenuTrigger>
  );
}
