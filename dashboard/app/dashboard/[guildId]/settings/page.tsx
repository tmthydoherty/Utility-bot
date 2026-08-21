import type { Metadata } from "next";
import Link from "next/link";
import { ChevronRight } from "lucide-react";

import { getActiveSession } from "@/auth";
import { isOwnerId } from "@/lib/auth/owner";
import { formatRelative } from "@/lib/utils";
import { navItems } from "@/lib/nav";
import { SESSION_MAX_AGE, REVALIDATE_INTERVAL_MS } from "@/auth.config";
import { Avatar } from "@/components/ui/avatar";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { PageHeader } from "@/components/ui/page-header";
import { ThemeControls } from "@/components/shell/theme-controls";
import { SignOutButton } from "@/components/auth/sign-out-button";

export const metadata: Metadata = { title: "Settings" };

const GRANT_LABELS: Record<string, string> = {
  owner: "You own this server",
  administrator: "You have the Administrator permission",
  "admin-role": "You hold the bot's admin role",
};

const GRANT_SHORT: Record<string, string> = {
  owner: "Server owner",
  administrator: "Administrator permission",
  "admin-role": "Admin role",
};

export default async function SettingsPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const session = await getActiveSession();

  // The operator-facing sections (Audit log, Bot control) that no longer sit on
  // the main rail. They live here on every viewport now, not just the phone —
  // the sidebar deliberately stays down to the everyday destinations.
  const otherSections = navItems(guildId, isOwnerId(session?.user.id)).filter(
    (item) => item.secondary,
  );

  return (
    <div className="space-y-6">
      <PageHeader
        title="Settings"
        description="Your account, and how the dashboard looks and behaves for you."
      />

      {otherSections.length > 0 && (
        <Card className="p-2">
          <h2 className="px-3 pb-1 pt-2 text-sm font-semibold uppercase tracking-wide text-fg-muted">
            Sections
          </h2>
          <ul>
            {otherSections.map((item) => (
              <li key={item.href}>
                <Link
                  href={item.href}
                  className="flex items-center gap-3 rounded-md px-3 py-3 transition-colors hover:bg-[var(--surface-hover)]"
                >
                  <Icon name={item.icon} className="size-[18px] shrink-0 text-fg-muted" />
                  <span className="min-w-0 flex-1">
                    <span className="block text-sm font-medium">{item.label}</span>
                    {item.description && (
                      <span className="block truncate text-xs text-fg-subtle">
                        {item.description}
                      </span>
                    )}
                  </span>
                  <ChevronRight className="size-4 shrink-0 text-fg-subtle" aria-hidden />
                </Link>
              </li>
            ))}
          </ul>
        </Card>
      )}

      <Card className="p-5 sm:p-6">
        <h2 className="mb-5 text-sm font-semibold uppercase tracking-wide text-fg-muted">
          Appearance
        </h2>
        <ThemeControls />
      </Card>

      <Card className="p-5 sm:p-6">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">Account</h2>

        <div className="mt-4 flex items-center gap-3">
          <Avatar src={session?.user.image ?? null} name={session?.user.name ?? "You"} size="lg" />
          <div className="min-w-0">
            <p className="truncate font-medium">{session?.user.name ?? "—"}</p>
            <p className="text-xs text-fg-muted">
              {(session?.user.grantedBy && GRANT_SHORT[session.user.grantedBy]) ?? "Signed in"}
            </p>
          </div>
        </div>

        <dl className="mt-5 space-y-3 border-t border-[var(--border)] pt-5 text-sm">
          <div className="flex flex-wrap justify-between gap-2">
            <dt className="text-fg-muted">Access granted because</dt>
            <dd className="text-right font-medium">
              {(session?.user.grantedBy && GRANT_LABELS[session.user.grantedBy]) ?? "—"}
            </dd>
          </div>
          <div className="flex flex-wrap justify-between gap-2">
            <dt className="text-fg-muted">Last verified with Discord</dt>
            <dd className="font-medium">
              {session?.user.checkedAt ? formatRelative(session.user.checkedAt) : "—"}
            </dd>
          </div>
        </dl>

        <p className="mt-4 text-xs leading-relaxed text-fg-subtle">
          Your access is re-checked against Discord at most every{" "}
          {Math.round(REVALIDATE_INTERVAL_MS / 60000)} minutes, and the session ends after{" "}
          {Math.round(SESSION_MAX_AGE / 3600)} hours of inactivity. Losing the admin role signs you
          out at the next check — no restart needed.
        </p>

        <div className="mt-5">
          <SignOutButton />
        </div>
      </Card>
    </div>
  );
}
