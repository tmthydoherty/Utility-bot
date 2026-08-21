"use client";

import * as React from "react";
import { signOut } from "next-auth/react";

import { cn } from "@/lib/utils";
import { GuildProvider, type GuildContextValue } from "@/components/providers/guild-provider";
import { CommandPalette } from "./command-palette";
import { MobileNav } from "./mobile-nav";
import { Sidebar } from "./sidebar";
import { Topbar } from "./topbar";

const SIDEBAR_STORAGE_KEY = "vibey.sidebar-collapsed";

export interface ShellUser {
  name: string;
  avatarUrl: string | null;
  grantedBy: string | null;
}

/**
 * The authenticated layout.
 *
 * Owns the guild context, the sidebar's collapsed state, and the two
 * navigation presentations. Deliberately the only client boundary in the
 * layout tree — the pages inside it stay server components so a dashboard page
 * costs no JavaScript beyond what its own interactive bits need.
 */
export function AppShell({
  guild,
  user,
  isOwner,
  children,
}: {
  guild: GuildContextValue;
  user: ShellUser;
  /** Whether the viewer is the bot owner — gates the owner-only nav items. */
  isOwner: boolean;
  children: React.ReactNode;
}) {
  const [collapsed, setCollapsed] = React.useState(false);

  // Read after mount rather than during render: reading localStorage in the
  // initial state would make the server and client markup disagree.
  React.useEffect(() => {
    try {
      setCollapsed(localStorage.getItem(SIDEBAR_STORAGE_KEY) === "true");
    } catch {
      /* storage unavailable — the default is fine */
    }
  }, []);

  const toggle = () => {
    setCollapsed((current) => {
      const next = !current;
      try {
        localStorage.setItem(SIDEBAR_STORAGE_KEY, String(next));
      } catch {
        /* not worth failing a click over */
      }
      return next;
    });
  };

  const handleSignOut = () => {
    void signOut({ callbackUrl: "/" });
  };

  return (
    <GuildProvider value={guild}>
      <div
        className="min-h-dvh"
        // Published so anything fixed-position (the save bar) can align to the
        // content column instead of the viewport.
        style={{ ["--sidebar-width" as string]: collapsed ? "4.5rem" : "17rem" }}
      >
        <Sidebar collapsed={collapsed} onToggle={toggle} isOwner={isOwner} />

        <div className="shell-content flex min-h-dvh flex-col transition-[padding] duration-300 ease-out">
          <Topbar user={user} onSignOut={handleSignOut} />
          <main
            id="main"
            className={cn(
              "mx-auto w-full max-w-7xl flex-1 px-4 py-6 sm:px-6 sm:py-8",
              // Clears the mobile tab bar and the home indicator below it.
              "pb-[calc(env(safe-area-inset-bottom)+5rem)] lg:pb-10",
            )}
          >
            {children}
          </main>
        </div>

        <MobileNav />
        <CommandPalette isOwner={isOwner} />
      </div>
    </GuildProvider>
  );
}
