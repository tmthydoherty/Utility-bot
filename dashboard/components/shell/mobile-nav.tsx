"use client";

import * as React from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { motion } from "motion/react";
import { LogOut, MoreHorizontal } from "lucide-react";

import { cn } from "@/lib/utils";
import { SPRING } from "@/lib/motion";
import { isActive, navItems } from "@/lib/nav";
import { useGuild } from "@/components/providers/guild-provider";
import { Avatar } from "@/components/ui/avatar";
import { Icon } from "@/components/ui/icon";
import { Sheet } from "@/components/ui/sheet";
import { ThemeControls } from "./theme-controls";

/**
 * Mobile bottom navigation.
 *
 * Bottom rather than top because that is where thumbs are — the top of a
 * modern phone is the hardest part of the screen to reach one-handed, and a
 * dashboard is used one-handed. Everything past the four primary destinations
 * lives behind "More", which opens a sheet rather than navigating, so the
 * user never loses their place to reach a setting.
 */
export function MobileNav({
  user,
  onSignOut,
}: {
  user: { name: string; avatarUrl: string | null; grantedBy: string | null };
  onSignOut: () => void;
}) {
  const pathname = usePathname();
  const guild = useGuild();
  const [moreOpen, setMoreOpen] = React.useState(false);

  const items = navItems(guild.id).filter((item) => item.primary);

  return (
    <>
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
                  <Icon
                    name={item.icon}
                    className={cn("size-5", active && "text-[var(--accent)]")}
                  />
                  <span className="truncate">{item.label}</span>
                </Link>
              </li>
            );
          })}
          <li className="flex-1">
            <button
              type="button"
              onClick={() => setMoreOpen(true)}
              className="flex h-14 w-full flex-col items-center justify-center gap-1 px-1 text-[10px] font-medium text-fg-subtle"
              aria-label="More options"
            >
              <MoreHorizontal className="size-5" aria-hidden />
              <span>More</span>
            </button>
          </li>
        </ul>
      </nav>

      <Sheet
        open={moreOpen}
        onOpenChange={setMoreOpen}
        title="Account & appearance"
        description="Signed in through Discord."
      >
        <div className="space-y-6">
          <div className="glass flex items-center gap-3 rounded-lg p-3">
            <Avatar src={user.avatarUrl} name={user.name} size="lg" />
            <div className="min-w-0">
              <p className="truncate font-medium">{user.name}</p>
              <p className="text-xs text-fg-muted">
                {user.grantedBy === "owner"
                  ? "Server owner"
                  : user.grantedBy === "administrator"
                    ? "Administrator permission"
                    : "Admin role"}
              </p>
            </div>
          </div>

          <ThemeControls />

          <button
            type="button"
            onClick={onSignOut}
            className={cn(
              "flex h-12 w-full items-center gap-3 rounded-md px-3 text-sm font-medium",
              "text-[var(--danger)] transition-colors hover:bg-[var(--danger-soft)]",
            )}
          >
            <LogOut className="size-[18px]" aria-hidden />
            Sign out
          </button>
        </div>
      </Sheet>
    </>
  );
}
