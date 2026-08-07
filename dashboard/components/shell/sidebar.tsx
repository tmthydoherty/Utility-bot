"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { motion } from "motion/react";
import { PanelLeftClose, PanelLeftOpen } from "lucide-react";

import { cn } from "@/lib/utils";
import { SPRING } from "@/lib/motion";
import { isActive, navItems } from "@/lib/nav";
import { useGuild } from "@/components/providers/guild-provider";
import { Avatar } from "@/components/ui/avatar";
import { Icon } from "@/components/ui/icon";
import { Tooltip } from "@/components/ui/tooltip";

/**
 * Desktop sidebar.
 *
 * The active indicator is a single element that slides between items via a
 * shared `layoutId`, rather than a background that fades in on one item and
 * out on another. The movement is the point — it shows *where the selection
 * went*, which a crossfade cannot.
 */
export function Sidebar({
  collapsed,
  onToggle,
}: {
  collapsed: boolean;
  onToggle: () => void;
}) {
  const pathname = usePathname();
  const guild = useGuild();
  const items = navItems(guild.id);

  return (
    <aside
      className={cn(
        "fixed inset-y-0 left-0 z-30 hidden lg:flex",
        "flex-col border-r border-[var(--border)] bg-[var(--bg-elevated)]/60 backdrop-blur-xl",
        "transition-[width] duration-300 ease-[cubic-bezier(0.22,1,0.36,1)]",
      )}
      // Width is inline so the same value can be published as a CSS variable
      // on the shell, which is what the save bar aligns itself against.
      style={{ width: collapsed ? "4.5rem" : "17rem" }}
      aria-label="Main navigation"
    >
      <div className="flex h-16 items-center gap-3 px-4">
        <Avatar src={guild.iconUrl} name={guild.name} rounded="md" size="md" />
        {!collapsed && (
          <div className="min-w-0 flex-1">
            <p className="truncate text-sm font-semibold leading-tight">{guild.name}</p>
            <p className="text-xs text-fg-subtle">Vibey dashboard</p>
          </div>
        )}
      </div>

      <div className="rule mx-4" />

      <nav className="flex-1 space-y-1 overflow-y-auto p-3">
        {items.map((item) => {
          const active = isActive(item.href, pathname, item.label === "Overview");
          const link = (
            <Link
              key={item.href}
              href={item.href}
              aria-current={active ? "page" : undefined}
              className={cn(
                "group relative flex h-11 items-center gap-3 rounded-md px-3",
                "text-sm font-medium transition-colors duration-150",
                active ? "text-fg" : "text-fg-muted hover:text-fg",
                collapsed && "justify-center px-0",
              )}
            >
              {active && (
                <motion.span
                  layoutId="sidebar-active"
                  transition={SPRING}
                  className="absolute inset-0 rounded-md bg-[var(--accent-soft)]"
                />
              )}
              {active && (
                <motion.span
                  layoutId="sidebar-active-edge"
                  transition={SPRING}
                  className="accent-gradient absolute left-0 top-1/2 h-6 w-[3px] -translate-y-1/2 rounded-r-full"
                />
              )}
              <Icon
                name={item.icon}
                className={cn(
                  "relative z-10 size-[18px] shrink-0",
                  active && "text-[var(--accent)]",
                )}
              />
              {!collapsed && <span className="relative z-10 truncate">{item.label}</span>}
            </Link>
          );

          // Collapsed, the label is gone, so the tooltip is the only way to
          // read the item — and it is keyboard-reachable, not hover-only.
          return collapsed ? (
            <Tooltip key={item.href} content={item.label} side="right">
              {link}
            </Tooltip>
          ) : (
            link
          );
        })}
      </nav>

      <div className="p-3">
        <button
          type="button"
          onClick={onToggle}
          className={cn(
            "flex h-10 w-full items-center gap-3 rounded-md px-3 text-sm text-fg-subtle",
            "transition-colors hover:bg-[var(--surface)] hover:text-fg",
            collapsed && "justify-center px-0",
          )}
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          {collapsed ? (
            <PanelLeftOpen className="size-[18px]" aria-hidden />
          ) : (
            <>
              <PanelLeftClose className="size-[18px]" aria-hidden />
              <span>Collapse</span>
            </>
          )}
        </button>
      </div>
    </aside>
  );
}
