import type { Metadata } from "next";

import { getActiveSession } from "@/auth";
import { formatRelative } from "@/lib/utils";
import { SESSION_MAX_AGE, REVALIDATE_INTERVAL_MS } from "@/auth.config";
import { Card } from "@/components/ui/card";
import { PageHeader } from "@/components/ui/page-header";
import { ThemeControls } from "@/components/shell/theme-controls";
import { SignOutButton } from "@/components/auth/sign-out-button";

export const metadata: Metadata = { title: "Settings" };

const GRANT_LABELS: Record<string, string> = {
  owner: "You own this server",
  administrator: "You have the Administrator permission",
  "admin-role": "You hold the bot's admin role",
};

export default async function SettingsPage() {
  const session = await getActiveSession();

  return (
    <div className="space-y-6">
      <PageHeader
        title="Settings"
        description="How the dashboard looks and behaves for you. These are stored in this browser only."
      />

      <Card className="p-5 sm:p-6">
        <h2 className="mb-5 text-sm font-semibold uppercase tracking-wide text-fg-muted">
          Appearance
        </h2>
        <ThemeControls />
      </Card>

      <Card className="p-5 sm:p-6">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">Session</h2>

        <dl className="mt-4 space-y-3 text-sm">
          <div className="flex flex-wrap justify-between gap-2">
            <dt className="text-fg-muted">Signed in as</dt>
            <dd className="font-medium">{session?.user.name ?? "—"}</dd>
          </div>
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
