"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Command } from "cmdk";
import { AnimatePresence, motion } from "motion/react";
import { Search } from "lucide-react";

import { cn } from "@/lib/utils";
import { transitions } from "@/lib/motion";
import { navItems } from "@/lib/nav";
import { MODULES } from "@/lib/schema/modules";
import { CATEGORY_LABELS } from "@/lib/schema/types";
import { useGuild } from "@/components/providers/guild-provider";
import { Icon } from "@/components/ui/icon";

/**
 * Command palette.
 *
 * With ~35 modules, the fastest route to any one of them is typing its name.
 * Keyboard-only by design — there is a search field on the modules page for
 * touch, and a floating "⌘K" button on a phone would be a hit target
 * permanently covering content.
 */
export function CommandPalette() {
  const [open, setOpen] = React.useState(false);
  const router = useRouter();
  const guild = useGuild();

  React.useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "k" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        setOpen((current) => !current);
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  const go = (href: string) => {
    setOpen(false);
    router.push(href);
  };

  return (
    <AnimatePresence>
      {open && (
        <Command.Dialog
          open
          onOpenChange={setOpen}
          label="Command palette"
          // Radix's Dialog underneath handles the focus trap; the wrapper below
          // is purely presentational.
          className="fixed inset-0 z-[90]"
        >
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={transitions.standard}
            className="fixed inset-0 bg-black/60 backdrop-blur-sm"
            onClick={() => setOpen(false)}
          />
          <motion.div
            initial={{ opacity: 0, y: -12, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -8, scale: 0.98 }}
            transition={transitions.standard}
            className={cn(
              "fixed left-1/2 top-[12vh] z-10 w-[min(94vw,600px)] -translate-x-1/2",
              "glass glass-highlight overflow-hidden rounded-xl shadow-[var(--elev-4)]",
            )}
          >
            <div className="flex items-center gap-3 border-b border-[var(--border)] px-4">
              <Search className="size-4 shrink-0 text-fg-subtle" aria-hidden />
              <Command.Input
                autoFocus
                placeholder="Jump to a page or module…"
                className="h-13 w-full bg-transparent py-4 text-base outline-none placeholder:text-fg-subtle"
              />
              <kbd className="hidden shrink-0 rounded border border-[var(--border-strong)] px-1.5 py-0.5 font-mono text-[10px] text-fg-subtle sm:block">
                ESC
              </kbd>
            </div>

            <Command.List className="max-h-[52vh] overflow-y-auto overscroll-contain p-2">
              <Command.Empty className="px-3 py-10 text-center text-sm text-fg-muted">
                Nothing matches that.
              </Command.Empty>

              <Command.Group
                heading="Pages"
                className="[&_[cmdk-group-heading]]:px-2 [&_[cmdk-group-heading]]:pb-1 [&_[cmdk-group-heading]]:pt-2 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:font-medium [&_[cmdk-group-heading]]:uppercase [&_[cmdk-group-heading]]:tracking-wide [&_[cmdk-group-heading]]:text-fg-subtle"
              >
                {navItems(guild.id).map((item) => (
                  <PaletteItem
                    key={item.href}
                    icon={item.icon}
                    label={item.label}
                    hint={item.description}
                    onSelect={() => go(item.href)}
                  />
                ))}
              </Command.Group>

              <Command.Group
                heading="Modules"
                className="[&_[cmdk-group-heading]]:px-2 [&_[cmdk-group-heading]]:pb-1 [&_[cmdk-group-heading]]:pt-3 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:font-medium [&_[cmdk-group-heading]]:uppercase [&_[cmdk-group-heading]]:tracking-wide [&_[cmdk-group-heading]]:text-fg-subtle"
              >
                {MODULES.map((module) => (
                  <PaletteItem
                    key={module.id}
                    icon={module.icon}
                    label={module.name}
                    hint={CATEGORY_LABELS[module.category]}
                    onSelect={() => go(`/dashboard/${guild.id}/modules/${module.id}`)}
                  />
                ))}
              </Command.Group>
            </Command.List>
          </motion.div>
        </Command.Dialog>
      )}
    </AnimatePresence>
  );
}

function PaletteItem({
  icon,
  label,
  hint,
  onSelect,
}: {
  icon: string;
  label: string;
  hint?: string;
  onSelect: () => void;
}) {
  return (
    <Command.Item
      value={label}
      onSelect={onSelect}
      className={cn(
        "flex cursor-pointer items-center gap-3 rounded-md px-2.5 py-2.5 text-sm",
        "data-[selected=true]:bg-[var(--surface-hover)]",
      )}
    >
      <Icon name={icon} className="size-4 shrink-0 text-fg-muted" />
      <span className="min-w-0 flex-1 truncate">{label}</span>
      {hint && <span className="shrink-0 text-xs text-fg-subtle">{hint}</span>}
    </Command.Item>
  );
}
