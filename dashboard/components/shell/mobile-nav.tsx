"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { motion } from "motion/react";

import { cn } from "@/lib/utils";
import { SPRING } from "@/lib/motion";
import { isActive, navItems } from "@/lib/nav";
import { useGuild } from "@/components/providers/guild-provider";
import { Icon } from "@/components/ui/icon";

/**
 * Mobile bottom navigation.
 *
 * Bottom rather than top because that is where thumbs are — the top of a
 * modern phone is the hardest part of the screen to reach one-handed, and a
 * dashboard is used one-handed.
 *
 * There is no separate "More" sheet any more: it used to re-implement the theme
 * controls, the account block and sign-out that the Settings page already owns,
 * which was two places to change one thing. Settings is a primary tab, so
 * everything the sheet held now lives there — including links to the sections
 * that don't fit the tab bar (see the Settings page's "Other sections" card).
 */
export function MobileNav() {
  const pathname = usePathname();
  const guild = useGuild();

  const items = navItems(guild.id).filter((item) => item.primary);

  return (
    <nav
      aria-label="Main navigation"
      className={cn(
        "fixed inset-x-0 bottom-0 z-40 lg:hidden",
        "border-t border-[var(--border)] bg-[var(--bg-elevated)]/85 backdrop-blur-xl",
      )}
      // Keeps the row clear of the home indicator on gesture-nav phones.
      style={{ paddingBottom: "env(safe-area-inset-bottom)" }}
    >
      <ul className="flex items-stretch">
        {items.map((item) => {
          const active = isActive(item.href, pathname, item.label === "Overview");
          return (
            <li key={item.href} className="flex-1">
              <Link
                href={item.href}
                aria-current={active ? "page" : undefined}
                className={cn(
                  // 56px tall: comfortably above the 44px minimum, because a
                  // mis-tap here navigates away from unsaved work.
                  "relative flex h-14 flex-col items-center justify-center gap-1 px-1",
                  "text-[10px] font-medium transition-colors",
                  active ? "text-fg" : "text-fg-subtle",
                )}
              >
                {active && (
                  <motion.span
                    layoutId="mobile-active"
                    transition={SPRING}
                    className="accent-gradient absolute inset-x-4 top-0 h-[2px] rounded-full"
                  />
                )}
                <Icon name={item.icon} className={cn("size-5", active && "text-[var(--accent)]")} />
                <span className="truncate">{item.label}</span>
              </Link>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}
