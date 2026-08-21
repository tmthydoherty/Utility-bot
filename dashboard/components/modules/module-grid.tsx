"use client";

import * as React from "react";
import Link from "next/link";
import { AnimatePresence, motion } from "motion/react";
import { Search, X } from "lucide-react";

import { cn } from "@/lib/utils";
import { SPRING, listItemVariants } from "@/lib/motion";
import { type ModuleSchema } from "@/lib/schema/types";
import { isSupported } from "@/lib/schema/modules";
import { StatusDot } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Icon } from "@/components/ui/icon";
import { Input } from "@/components/ui/input";

/**
 * The module catalogue.
 *
 * A flat, alphabetical list rather than category groups: with the supported
 * modules up front the eye scans a single A–Z run faster than it hops between
 * eight headed sections. Search stays because "the thing in economy that does
 * cards" is how people actually look; the category filter is gone. Filtering is
 * client-side because the whole list is already here.
 *
 * The cards show each module's on/off state but no longer carry the switch —
 * enabling and disabling lives on the module's own page, so the change is made
 * next to the settings it affects rather than from a dense grid.
 */
export function ModuleGrid({
  modules,
  guildId,
  initialStates,
}: {
  modules: ModuleSchema[];
  guildId: string;
  initialStates: Record<string, boolean>;
}) {
  const [query, setQuery] = React.useState("");

  const filtered = React.useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return modules;
    return modules.filter(
      (module) =>
        module.name.toLowerCase().includes(needle) ||
        module.description.toLowerCase().includes(needle) ||
        module.cog.toLowerCase().includes(needle),
    );
  }, [modules, query]);

  const byName = (a: ModuleSchema, b: ModuleSchema) => a.name.localeCompare(b.name);
  const supported = filtered.filter(isSupported).sort(byName);
  return (
    <div className="space-y-5">
      <div className="relative">
        <Search
          className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-fg-subtle"
          aria-hidden
        />
        <Input
          type="search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search modules…"
          aria-label="Search modules"
          className="pl-9 pr-9"
        />
        {query && (
          <button
            type="button"
            onClick={() => setQuery("")}
            aria-label="Clear search"
            className="absolute right-2 top-1/2 grid size-7 -translate-y-1/2 place-items-center rounded text-fg-subtle transition-colors hover:text-fg"
          >
            <X className="size-4" />
          </button>
        )}
      </div>

      {filtered.length === 0 ? (
        <EmptyState
          icon="Search"
          title="No modules match that"
          description="Try a shorter search."
        />
      ) : (
        <div className="space-y-8">
          {supported.length > 0 && (
            <motion.div layout className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
              <AnimatePresence mode="popLayout">
                {supported.map((module, index) => (
                  <motion.div
                    key={module.id}
                    layout
                    custom={index}
                    variants={listItemVariants}
                    initial="hidden"
                    animate="visible"
                    exit={{ opacity: 0, scale: 0.97, transition: { duration: 0.12 } }}
                    transition={SPRING}
                  >
                    <ModuleCard
                      module={module}
                      guildId={guildId}
                      enabled={initialStates[module.id] !== false}
                    />
                  </motion.div>
                ))}
              </AnimatePresence>
            </motion.div>
          )}
        </div>
      )}
    </div>
  );
}

function ModuleCard({
  module,
  guildId,
  enabled,
}: {
  module: ModuleSchema;
  guildId: string;
  enabled: boolean;
}) {
  // An external module (Automations) has its own builder page rather than a
  // settings form, but it's still a real module with an on/off state.
  const href = module.external
    ? `/dashboard/${guildId}${module.link ?? ""}`
    : `/dashboard/${guildId}/modules/${module.id}`;

  // An always-on module (Bot Presence) has no off state; it reads as active and
  // says "Always on" rather than Enabled/Disabled.
  const active = module.alwaysOn || enabled;
  const statusLabel = module.alwaysOn ? "Always on" : enabled ? "Enabled" : "Disabled";

  return (
    <Card interactive className="group relative flex h-full flex-col p-5">
      <div className="flex items-start gap-3">
        <div
          className={cn(
            "grid size-10 shrink-0 place-items-center rounded-lg transition-colors",
            active
              ? "bg-[var(--accent-soft)] text-[var(--accent)]"
              : "bg-[var(--surface-hover)] text-fg-subtle",
          )}
        >
          <Icon name={module.icon} className="size-5" />
        </div>

        <div className="min-w-0 flex-1">
          {/* Stretched link: the whole card is the target, but the switch below
              sits above it on the z-axis so it still gets its own clicks. */}
          <h3 className="font-semibold leading-tight">
            <Link
              href={href}
              className="after:absolute after:inset-0 after:content-['']"
            >
              {module.name}
            </Link>
          </h3>
          <p className="mt-1 line-clamp-2 text-sm leading-relaxed text-fg-muted">
            {module.description}
          </p>
        </div>
      </div>

      {/* Status footer — read-only. The switch lives on the module's own page
          now, so the grid shows state without inviting an accidental toggle.
          Every card reads the same, including the detailed modules (Ticketing,
          Automations) that link to their own builder rather than a form. */}
      <div className="mt-4 flex items-center gap-2">
        <StatusDot active={active} label={statusLabel} />
        <span className="text-xs text-fg-subtle">{statusLabel}</span>
      </div>
    </Card>
  );
}

