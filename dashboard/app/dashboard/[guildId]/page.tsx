import type { Metadata } from "next";
import Link from "next/link";
import { ArrowRight } from "lucide-react";

import { formatNumber } from "@/lib/utils";
import { loadGuild } from "@/lib/guild";
import { readModuleStates, stats, STATS_ARE_SAMPLE } from "@/lib/bot/adapter";
import { MODULES } from "@/lib/schema/modules";
import { getGuild } from "@/lib/discord/rest";
import { ActivityChart, type ActivityPoint } from "@/components/charts/activity-chart";
import { Badge, StatusDot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { PageHeader, SampleDataNotice } from "@/components/ui/page-header";
import { Reveal } from "@/components/ui/reveal";
import { StatTile } from "@/components/ui/stat-tile";

export const metadata: Metadata = { title: "Overview" };

export default async function OverviewPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  const [guild, discordGuild] = await Promise.all([loadGuild(guildId), getGuild(guildId)]);
  const moduleStates = readModuleStates(guildId);

  const configurable = MODULES.filter((module) => module.configurable);
  const active = MODULES.filter((module) => moduleStates.get(module.id)?.enabled !== false).length;

  const data = stats(guildId, guild.memberCount, discordGuild.approximate_presence_count ?? 0, {
    active,
    total: MODULES.length,
  });

  // 14 days of labels ending today, generated on the server so the chart's
  // x-axis doesn't shift depending on the reader's clock.
  const now = new Date();
  const chart: ActivityPoint[] = data.messagesTrend.map((value, index) => {
    const date = new Date(now);
    date.setDate(date.getDate() - (data.messagesTrend.length - 1 - index));
    return {
      label: date.toLocaleDateString("en-US", { month: "short", day: "numeric" }),
      value,
    };
  });

  return (
    <div className="space-y-6">
      <PageHeader
        title={guild.name}
        description="Everything Vibey is doing in this server, at a glance."
        action={
          <Button asChild variant="secondary">
            <Link href={`/dashboard/${guildId}/modules`}>
              Manage modules
              <ArrowRight aria-hidden />
            </Link>
          </Button>
        }
      />

      {STATS_ARE_SAMPLE && <SampleDataNotice />}

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {[
          {
            label: "Members",
            value: data.members,
            icon: "Users",
            delta: 1.8,
            trend: data.messagesTrend,
          },
          { label: "Online now", value: data.online, icon: "Activity" },
          {
            label: "Messages this week",
            value: data.messages7d,
            icon: "MessagesSquare",
            delta: -4.2,
            trend: data.messagesTrend,
          },
          {
            label: "New members",
            value: data.newMembers7d,
            icon: "Sparkles",
            delta: 12.4,
          },
        ].map((tile, index) => (
          <Reveal key={tile.label} delay={index}>
            <StatTile {...tile} />
          </Reveal>
        ))}
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <Reveal className="lg:col-span-2">
          <Card className="h-full p-5 sm:p-6">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 className="font-semibold tracking-tight">Message activity</h2>
                <p className="text-sm text-fg-muted">Last 14 days</p>
              </div>
              <Badge variant="accent">
                {formatNumber(data.messages7d)} this week
              </Badge>
            </div>
            <div className="mt-5">
              <ActivityChart data={chart} />
            </div>
          </Card>
        </Reveal>

        <Reveal delay={1}>
          <Card className="flex h-full flex-col p-5 sm:p-6">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 className="font-semibold tracking-tight">Modules</h2>
                <p className="text-sm text-fg-muted">
                  {data.activeModules} of {data.totalModules} enabled
                </p>
              </div>
            </div>

            <ul className="mt-5 flex-1 space-y-1">
              {configurable.map((module) => {
                const enabled = moduleStates.get(module.id)?.enabled !== false;
                return (
                  <li key={module.id}>
                    <Link
                      href={`/dashboard/${guildId}/modules/${module.id}`}
                      className="flex items-center gap-3 rounded-md px-2 py-2.5 transition-colors hover:bg-[var(--surface-hover)]"
                    >
                      <Icon name={module.icon} className="size-4 shrink-0 text-fg-muted" />
                      <span className="min-w-0 flex-1 truncate text-sm">{module.name}</span>
                      <StatusDot active={enabled} label={enabled ? "Enabled" : "Disabled"} />
                    </Link>
                  </li>
                );
              })}
            </ul>

            <Button asChild variant="ghost" size="sm" className="mt-3 w-full">
              <Link href={`/dashboard/${guildId}/modules`}>
                See all {MODULES.length}
                <ArrowRight aria-hidden />
              </Link>
            </Button>
          </Card>
        </Reveal>
      </div>
    </div>
  );
}
