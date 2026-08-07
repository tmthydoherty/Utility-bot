"use client";

import * as React from "react";
import Link from "next/link";
import { AnimatePresence, motion } from "motion/react";
import { Search, X } from "lucide-react";

import { cn } from "@/lib/utils";
import { SPRING, listItemVariants } from "@/lib/motion";
import { CATEGORY_LABELS, type ModuleCategory, type ModuleSchema } from "@/lib/schema/types";
import { moduleGroups } from "@/lib/nav";
import { toggleModule } from "@/app/actions/settings";
import { Badge, StatusDot } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Icon } from "@/components/ui/icon";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { useToast } from "@/components/ui/toast";

/**
 * The module catalogue.
 *
 * Roughly thirty-five cards is past the point where scanning works, so the
 * page leads with search and a category filter rather than expecting the eye
 * to do it. Filtering is client-side because the whole list is already here —
 * a round trip per keystroke would be slower and would need a spinner.
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
  const toast = useToast();
  const [query, setQuery] = React.useState("");
  const [category, setCategory] = React.useState<ModuleCategory | "all">("all");
  const [states, setStates] = React.useState(initialStates);

  const filtered = React.useMemo(() => {
    const needle = query.trim().toLowerCase();
    return modules.filter((module) => {
      if (category !== "all" && module.category !== category) return false;
      if (!needle) return true;
      // Search the description and the cog path too — "trivia" is easy, but
      // "the thing in economy that does cards" is how people actually look.
      return (
        module.name.toLowerCase().includes(needle) ||
        module.description.toLowerCase().includes(needle) ||
        module.cog.toLowerCase().includes(needle)
      );
    });
  }, [modules, query, category]);

  const groups = moduleGroups(filtered);
  const categories = React.useMemo(
    () => moduleGroups(modules).map((group) => group.category),
    [modules],
  );

  const setEnabled = async (moduleId: string, enabled: boolean) => {
    // Optimistic: the switch moves now and rolls back if the server disagrees.
    // A toggle that waits on a round trip feels broken on mobile data.
    setStates((current) => ({ ...current, [moduleId]: enabled }));
    const result = await toggleModule(guildId, moduleId, enabled);
    if (!result.ok) {
      setStates((current) => ({ ...current, [moduleId]: !enabled }));
      toast.error("Couldn't change that", result.error);
    }
  };

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-3">
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

        {/* Horizontal scroll rather than wrapping: eight chips wrapping to
            three rows on a phone pushes the actual content off screen. */}
        <div className="-mx-4 overflow-x-auto px-4 pb-1 [scrollbar-width:none] sm:mx-0 sm:px-0 [&::-webkit-scrollbar]:hidden">
          <div className="flex w-max gap-2">
            <FilterChip active={category === "all"} onClick={() => setCategory("all")}>
              All
              <span className="ml-1.5 text-fg-subtle">{modules.length}</span>
            </FilterChip>
            {categories.map((key) => (
              <FilterChip key={key} active={category === key} onClick={() => setCategory(key)}>
                {CATEGORY_LABELS[key]}
              </FilterChip>
            ))}
          </div>
        </div>
      </div>

      {filtered.length === 0 ? (
        <EmptyState
          icon="Search"
          title="No modules match that"
          description="Try a shorter search, or clear the category filter."
        />
      ) : (
        <div className="space-y-8">
          {groups.map((group) => (
            <section key={group.category} className="space-y-3">
              <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-subtle">
                {group.label}
              </h2>
              <motion.div layout className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                <AnimatePresence mode="popLayout">
                  {group.modules.map((module, index) => (
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
                        enabled={states[module.id] !== false}
                        onToggle={(enabled) => void setEnabled(module.id, enabled)}
                      />
                    </motion.div>
                  ))}
                </AnimatePresence>
              </motion.div>
            </section>
          ))}
        </div>
      )}
    </div>
  );
}

function FilterChip({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={cn(
        "h-9 shrink-0 rounded-full px-3.5 text-sm font-medium transition-colors",
        active
          ? "bg-[var(--accent-soft)] text-[var(--accent)]"
          : "glass text-fg-muted hover:text-fg",
      )}
    >
      {children}
    </button>
  );
}

function ModuleCard({
  module,
  guildId,
  enabled,
  onToggle,
}: {
  module: ModuleSchema;
  guildId: string;
  enabled: boolean;
  onToggle: (enabled: boolean) => void;
}) {
  return (
    <Card interactive className="group relative flex h-full flex-col p-5">
      <div className="flex items-start gap-3">
        <div
          className={cn(
            "grid size-10 shrink-0 place-items-center rounded-lg transition-colors",
            enabled
              ? "bg-[var(--accent-soft)] text-[var(--accent)]"
              : "bg-[var(--surface-hover)] text-fg-subtle",
          )}
        >
          <Icon name={module.icon} className="size-5" />
        </div>

        <div className="min-w-0 flex-1">
          {/* Stretched link: the whole card is the target, but the switch and
              anything else interactive sits above it on the z-axis so it still
              gets its own clicks. */}
          <h3 className="font-semibold leading-tight">
            <Link
              href={`/dashboard/${guildId}/modules/${module.id}`}
              className="after:absolute after:inset-0 after:content-['']"
            >
              {module.name}
            </Link>
          </h3>
          <p className="mt-1 line-clamp-2 text-sm leading-relaxed text-fg-muted">
            {module.description}
          </p>
        </div>

        <div className="relative z-10 shrink-0">
          <Switch
            checked={enabled}
            onCheckedChange={onToggle}
            aria-label={`${enabled ? "Disable" : "Enable"} ${module.name}`}
          />
        </div>
      </div>

      <div className="mt-4 flex items-center gap-2">
        <StatusDot active={enabled} label={enabled ? "Enabled" : "Disabled"} />
        <span className="text-xs text-fg-subtle">{enabled ? "Enabled" : "Disabled"}</span>
        {!module.configurable && (
          <Badge variant="outline" className="ml-auto">
            Discord only
          </Badge>
        )}
      </div>
    </Card>
  );
}
