"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { ChevronRight, LogOut, Search } from "lucide-react";

import { cn } from "@/lib/utils";
import { useGuild } from "@/components/providers/guild-provider";
import { getModule } from "@/lib/schema/modules";
import { Avatar } from "@/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

/**
 * Top bar: where you are, and who you are.
 *
 * Breadcrumbs are derived from the URL rather than passed down from each page,
 * so a new route cannot forget to set them.
 */

interface Crumb {
  label: string;
  href?: string;
}

/**
 * Sections whose crumb is the same as their nav label and needs no lookup.
 *
 * Listed rather than derived from the URL segment, because "audit" has to read
 * as "Audit log" — and because a section missing from here shows the guild's
 * own name on every one of its pages, which is how the automations section
 * spent its first render claiming to be the Overview.
 */
const SECTION_LABELS: Record<string, string> = {
  automations: "Automations",
  modules: "Modules",
  audit: "Audit log",
  settings: "Settings",
};

function useCrumbs(guildId: string): Crumb[] {
  const pathname = usePathname();
  const base = `/dashboard/${guildId}`;
  const rest = pathname.startsWith(base) ? pathname.slice(base.length) : "";
  const segments = rest.split("/").filter(Boolean);

  const crumbs: Crumb[] = [{ label: "Overview", href: base }];
  if (segments.length === 0) return crumbs;

  const [first, second] = segments;
  if (!first) return crumbs;

  const label = SECTION_LABELS[first];
  if (!label) return crumbs;

  crumbs.push({ label, href: `${base}/${first}` });
  if (!second) return crumbs;

  if (first === "modules") {
    // Fall back to the raw segment so an unknown module still shows a sensible
    // trail rather than a blank crumb.
    crumbs.push({ label: getModule(second)?.name ?? second });
  } else if (first === "automations") {
    // An automation's name lives in the bot's database, not in the URL, and
    // this component is deliberately client-only. The page below already shows
    // the name as its heading, so the crumb says where you are instead of
    // repeating it.
    crumbs.push({ label: second === "new" ? "New" : "Edit" });
  }

  return crumbs;
}

export function Topbar({
  user,
  onSignOut,
}: {
  user: { name: string; avatarUrl: string | null; grantedBy: string | null };
  onSignOut: () => void;
}) {
  const guild = useGuild();
  const crumbs = useCrumbs(guild.id);
  const last = crumbs[crumbs.length - 1];

  return (
    <header
      className={cn(
        "sticky top-0 z-20 flex h-16 items-center gap-3 px-4 sm:px-6",
        "border-b border-[var(--border)] bg-[var(--bg)]/70 backdrop-blur-xl",
      )}
      style={{ paddingTop: "env(safe-area-inset-top)" }}
    >
      {/* Mobile: the guild identity, since there is no sidebar to carry it. */}
      <div className="flex min-w-0 items-center gap-2.5 lg:hidden">
        <Avatar src={guild.iconUrl} name={guild.name} rounded="md" size="sm" />
        <div className="min-w-0">
          <p className="truncate text-sm font-semibold leading-tight">{last?.label}</p>
          <p className="truncate text-[11px] text-fg-subtle">{guild.name}</p>
        </div>
      </div>

      {/* Desktop: the full trail. */}
      <nav aria-label="Breadcrumb" className="hidden min-w-0 flex-1 lg:block">
        <ol className="flex items-center gap-1.5 text-sm">
          {crumbs.map((crumb, index) => {
            const isLast = index === crumbs.length - 1;
            return (
              <li key={`${crumb.label}-${index}`} className="flex min-w-0 items-center gap-1.5">
                {index > 0 && (
                  <ChevronRight className="size-3.5 shrink-0 text-fg-subtle" aria-hidden />
                )}
                {crumb.href && !isLast ? (
                  <Link href={crumb.href} className="truncate text-fg-muted transition-colors hover:text-fg">
                    {crumb.label}
                  </Link>
                ) : (
                  <span className="truncate font-medium" aria-current="page">
                    {crumb.label}
                  </span>
                )}
              </li>
            );
          })}
        </ol>
      </nav>

      <div className="ml-auto flex items-center gap-2">
        {/* A hint, not a control — the shortcut is the interface. Hidden on
            touch, where there is no keyboard to press it on. */}
        <div
          className={cn(
            "hidden items-center gap-2 rounded-md border border-[var(--border)] px-2.5 py-1.5",
            "text-xs text-fg-subtle lg:flex",
          )}
          aria-hidden
        >
          <Search className="size-3.5" />
          <span>Search</span>
          <kbd className="rounded border border-[var(--border-strong)] px-1 font-mono text-[10px]">⌘K</kbd>
        </div>

        {/* modal={false} is deliberate. Radix's modal mode locks body scroll
            and compensates for the scrollbar it removes, which visibly shunts
            the whole page sideways the moment the menu opens. A small account
            menu does not need a focus trap or a scroll lock. */}
        <DropdownMenu modal={false}>
          <DropdownMenuTrigger asChild>
            <button
              type="button"
              className="rounded-full transition-transform active:scale-95"
              aria-label="Account menu"
            >
              <Avatar src={user.avatarUrl} name={user.name} size="md" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent>
            <DropdownMenuLabel>{user.name}</DropdownMenuLabel>
            <DropdownMenuItem disabled className="text-xs text-fg-subtle">
              {user.grantedBy === "owner"
                ? "Server owner"
                : user.grantedBy === "administrator"
                  ? "Administrator permission"
                  : "Admin role"}
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem destructive onSelect={onSignOut}>
              <LogOut aria-hidden />
              Sign out
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </header>
  );
}
