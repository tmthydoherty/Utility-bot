import type { Metadata } from "next";
import Link from "next/link";

import { controlConfigured, getHealth } from "@/lib/bot/control";
import { CONTROL_TILES } from "@/lib/control/tabs";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { PageHeader } from "@/components/ui/page-header";
import { StatTile } from "@/components/ui/stat-tile";

export const metadata: Metadata = { title: "Bot control" };

/** "3d 4h", "12m", "45s" — the coarsest unit that's still informative. */
function formatUptime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

export default async function ControlPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const configured = controlConfigured();
  const health = configured ? await getHealth() : null;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Bot control"
        description="Vibey's own controls — its health, its features, its logs, and the power switch. Owner-only, and every action is written to the audit log."
      />

      {!configured ? (
        <Card className="p-5 sm:p-6">
          <div className="flex items-start gap-3">
            <Icon name="AlertTriangle" className="mt-0.5 size-5 shrink-0 text-[var(--danger)]" />
            <div className="space-y-1 text-sm">
              <p className="font-medium">The control channel isn&apos;t configured yet.</p>
              <p className="text-fg-muted">
                Set <code className="rounded bg-[var(--surface)] px-1">VIBEY_CONTROL_TOKEN</code> in
                both the bot&apos;s <code className="rounded bg-[var(--surface)] px-1">.env</code> and the
                dashboard&apos;s <code className="rounded bg-[var(--surface)] px-1">.env.local</code>, then
                restart both. Until then this section can&apos;t reach Vibey.
              </p>
            </div>
          </div>
        </Card>
      ) : health ? (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <StatTile
            label="Status"
            value={health.status === "online" ? "Online" : "Starting"}
            icon="Activity"
          />
          <StatTile label="Uptime" value={formatUptime(health.uptimeSeconds)} icon="Clock" />
          <StatTile
            label="Gateway latency"
            value={health.latencyMs ?? "—"}
            unit={health.latencyMs != null ? "ms" : undefined}
            icon="Gauge"
          />
          <StatTile label="Servers" value={health.guilds} icon="Users" />
        </div>
      ) : (
        <Card className="p-5 sm:p-6">
          <div className="flex items-start gap-3">
            <Icon name="AlertTriangle" className="mt-0.5 size-5 shrink-0 text-[var(--danger)]" />
            <div className="space-y-1 text-sm">
              <p className="font-medium">Vibey isn&apos;t responding.</p>
              <p className="text-fg-muted">
                The control channel is configured, but the bot didn&apos;t answer — it may be down or
                restarting. The sections below will work again once it&apos;s back.
              </p>
            </div>
          </div>
        </Card>
      )}

      {health && (
        <p className="text-xs text-fg-subtle">
          {health.user} · {health.members.toLocaleString()} members · {health.loadedCogs} cogs
          loaded · discord.py {health.discordPy} · Python {health.python}
        </p>
      )}

      <div className="grid gap-4 sm:grid-cols-2">
        {CONTROL_TILES.map((tile) => (
          <Card key={tile.slug} interactive className="group relative p-5 sm:p-6">
            <div className="flex items-start gap-4">
              <div className="grid size-11 shrink-0 place-items-center rounded-lg bg-[var(--accent-soft)] text-[var(--accent)]">
                <Icon name={tile.icon} className="size-5" />
              </div>
              <div className="min-w-0 flex-1">
                <h2 className="font-semibold leading-tight">
                  <Link
                    href={`/dashboard/${guildId}/control/${tile.slug}`}
                    className="after:absolute after:inset-0 after:content-['']"
                  >
                    {tile.title}
                  </Link>
                </h2>
                <p className="mt-1 text-sm leading-relaxed text-fg-muted">{tile.description}</p>
              </div>
              <Icon
                name="ArrowRight"
                className="size-4 shrink-0 text-fg-subtle transition-transform duration-200 group-hover:translate-x-0.5"
              />
            </div>
          </Card>
        ))}
      </div>
    </div>
  );
}
