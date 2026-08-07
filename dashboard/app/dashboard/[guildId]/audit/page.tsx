import type { Metadata } from "next";

import { formatRelative } from "@/lib/utils";
import { countAudit, listAudit } from "@/lib/db/audit";
import { getModule } from "@/lib/schema/modules";
import { loadGuild } from "@/lib/guild";
import { ChannelType } from "@/lib/discord/types";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";

export const metadata: Metadata = { title: "Audit log" };

const ACTION_LABELS: Record<string, string> = {
  "settings.update": "Updated settings",
  "module.toggle": "Toggled module",
};

/**
 * Renders a stored value in a way that survives every type it might be.
 *
 * Snowflakes are resolved to names where possible. An audit line reading
 * "empty → 1439727797017907301" is technically complete and practically
 * useless — the whole point of the log is being able to read it later and
 * understand what happened without going and looking each ID up.
 */
function renderValue(value: unknown, names: Map<string, string>): string {
  if (value === null || value === undefined || value === "") return "empty";
  if (typeof value === "boolean") return value ? "on" : "off";

  if (Array.isArray(value)) {
    if (value.length === 0) return "empty";
    const resolved = value.map((v) => names.get(String(v)) ?? String(v));
    // Past three, the list is longer than the line it sits on.
    return resolved.length > 3
      ? `${resolved.slice(0, 3).join(", ")} +${resolved.length - 3} more`
      : resolved.join(", ");
  }

  return names.get(String(value)) ?? String(value);
}

export default async function AuditPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const entries = listAudit(guildId, 100);
  const total = countAudit(guildId);

  // Snowflake -> readable name, built once for the whole page. The guild is
  // already cached from the layout's fetch, so this costs nothing.
  const guild = await loadGuild(guildId);
  const names = new Map<string, string>();
  for (const channel of guild.channels) {
    const prefix = channel.type === ChannelType.GuildVoice ? "🔊" : "#";
    names.set(channel.id, `${prefix}${channel.name}`);
  }
  for (const role of guild.roles) names.set(role.id, `@${role.name}`);

  // Rendered on the server, so relative times need a fixed reference — without
  // one, every row would be computed against a slightly different "now".
  const now = Date.now();

  return (
    <div className="space-y-6">
      <PageHeader
        title="Audit log"
        description="Every change made from this dashboard, with who made it and exactly what moved."
        action={total > 0 ? <Badge variant="neutral">{total} recorded</Badge> : undefined}
      />

      {entries.length === 0 ? (
        <Card>
          <EmptyState
            icon="ScrollText"
            title="Nothing recorded yet"
            description="Changes you make from the dashboard will appear here. Changes made through Discord commands are logged by the bot's own audit cog."
          />
        </Card>
      ) : (
        <Card className="divide-y divide-[var(--border)] overflow-hidden">
          {entries.map((entry) => {
            const moduleSchema = entry.target ? getModule(entry.target) : undefined;
            const changes = Object.entries(entry.changes ?? {});

            return (
              <div key={entry.id} className="p-4 sm:p-5">
                <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                  <span className="font-medium">{entry.actorName}</span>
                  <span className="text-sm text-fg-muted">
                    {ACTION_LABELS[entry.action] ?? entry.action}
                  </span>
                  {moduleSchema && <Badge variant="accent">{moduleSchema.name}</Badge>}
                  <span className="ml-auto text-xs text-fg-subtle">
                    {formatRelative(entry.createdAt, now)}
                  </span>
                </div>

                {changes.length > 0 && (
                  <ul className="mt-3 space-y-1">
                    {changes.map(([key, change]) => (
                      <li key={key} className="flex flex-wrap items-center gap-2 text-xs">
                        <code className="rounded bg-[var(--surface-hover)] px-1.5 py-0.5 font-mono text-fg-muted">
                          {key}
                        </code>
                        <span className="text-fg-subtle line-through">
                          {renderValue(change.from, names)}
                        </span>
                        <span className="text-fg-subtle" aria-hidden>
                          →
                        </span>
                        <span className="text-fg">{renderValue(change.to, names)}</span>
                      </li>
                    ))}
                  </ul>
                )}

                {entry.ip && (
                  <p className="mt-3 font-mono text-[11px] text-fg-subtle">from {entry.ip}</p>
                )}
              </div>
            );
          })}
        </Card>
      )}
    </div>
  );
}
