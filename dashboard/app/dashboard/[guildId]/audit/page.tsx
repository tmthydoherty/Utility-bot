import type { Metadata } from "next";
import Link from "next/link";
import { ChevronLeft, ChevronRight } from "lucide-react";

import { formatRelative } from "@/lib/utils";
import {
  countAudit,
  countAuditFiltered,
  listAuditActors,
  listAuditCategories,
  listAuditFiltered,
} from "@/lib/db/audit";
import { describeAction } from "@/lib/audit/labels";
import { getModule } from "@/lib/schema/modules";
import { loadGuild } from "@/lib/guild";
import { resolveUsers } from "@/lib/bot/directory";
import { ChannelType } from "@/lib/discord/types";
import { Avatar } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { PageHeader } from "@/components/ui/page-header";
import { AuditFilterBar } from "@/components/audit/filter-bar";

export const metadata: Metadata = { title: "Audit log" };

const PAGE_SIZE = 50;

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

function first(value: string | string[] | undefined): string | null {
  const v = Array.isArray(value) ? value[0] : value;
  return v && v.length > 0 ? v : null;
}

export default async function AuditPage({
  params,
  searchParams,
}: {
  params: Promise<{ guildId: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { guildId } = await params;
  const sp = await searchParams;

  const actorId = first(sp.actor);
  const category = first(sp.category);
  const page = Math.max(1, Number(first(sp.page) ?? "1") || 1);
  const offset = (page - 1) * PAGE_SIZE;

  const filter = { actorId, category };
  const entries = listAuditFiltered(guildId, { ...filter, limit: PAGE_SIZE, offset });
  const matching = countAuditFiltered(guildId, filter);
  const total = countAudit(guildId);
  const actors = listAuditActors(guildId);
  const categories = listAuditCategories(guildId);

  // Snowflake -> readable name, built once for the whole page. The guild is
  // already cached from the layout's fetch, so this costs nothing.
  const guild = await loadGuild(guildId);
  const names = new Map<string, string>();
  for (const channel of guild.channels) {
    const prefix = channel.type === ChannelType.GuildVoice ? "🔊" : "#";
    names.set(channel.id, `${prefix}${channel.name}`);
  }
  for (const role of guild.roles) names.set(role.id, `@${role.name}`);

  // Actor avatars: one batched roster lookup for every distinct person on this
  // page, so a name carries a face without a call per row.
  const people = await resolveUsers(
    guildId,
    entries.map((e) => e.actorId),
  );

  // Rendered on the server, so relative times need a fixed reference — without
  // one, every row would be computed against a slightly different "now".
  const now = Date.now();

  const basePath = `/dashboard/${guildId}/audit`;
  const totalPages = Math.max(1, Math.ceil(matching / PAGE_SIZE));
  const pageHref = (n: number) => {
    const p = new URLSearchParams();
    if (actorId) p.set("actor", actorId);
    if (category) p.set("category", category);
    if (n > 1) p.set("page", String(n));
    const qs = p.toString();
    return qs ? `${basePath}?${qs}` : basePath;
  };

  return (
    <div className="space-y-6">
      <PageHeader
        title="Audit log"
        description="Every change made from this dashboard, with who made it and exactly what moved."
        action={total > 0 ? <Badge variant="neutral">{total} recorded</Badge> : undefined}
      />

      {total === 0 ? (
        <Card>
          <EmptyState
            icon="ScrollText"
            title="Nothing recorded yet"
            description="Changes you make from the dashboard will appear here. Changes made through Discord commands are logged by the bot's own audit cog."
          />
        </Card>
      ) : (
        <>
          <AuditFilterBar
            basePath={basePath}
            exportPath={`${basePath}/export`}
            actors={actors}
            categories={categories}
            activeActor={actorId}
            activeCategory={category}
          />

          {entries.length === 0 ? (
            <Card>
              <EmptyState
                icon="Search"
                title="No changes match these filters"
                description="Try a different person or area, or clear the filters."
              />
            </Card>
          ) : (
            <Card className="divide-y divide-[var(--border)] overflow-hidden">
              {entries.map((entry) => {
                const moduleSchema = entry.target ? getModule(entry.target) : undefined;
                const changes = Object.entries(entry.changes ?? {});
                const actor = people.get(entry.actorId);

                return (
                  <div key={entry.id} className="p-4 sm:p-5">
                    <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                      <Avatar src={actor?.avatar} name={entry.actorName} size="sm" />
                      <span className="font-medium">{entry.actorName}</span>
                      <span className="text-sm text-fg-muted">{describeAction(entry.action)}</span>
                      {moduleSchema && <Badge variant="accent">{moduleSchema.name}</Badge>}
                      <span
                        className="ml-auto text-xs text-fg-subtle"
                        title={new Date(entry.createdAt).toLocaleString()}
                      >
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

          {totalPages > 1 && (
            <nav className="flex items-center justify-between text-sm" aria-label="Audit log pages">
              <PageLink href={pageHref(page - 1)} disabled={page <= 1} rel="prev">
                <ChevronLeft className="size-4" aria-hidden />
                Newer
              </PageLink>
              <span className="text-fg-subtle">
                Page {page} of {totalPages}
              </span>
              <PageLink href={pageHref(page + 1)} disabled={page >= totalPages} rel="next">
                Older
                <ChevronRight className="size-4" aria-hidden />
              </PageLink>
            </nav>
          )}
        </>
      )}
    </div>
  );
}

function PageLink({
  href,
  disabled,
  rel,
  children,
}: {
  href: string;
  disabled: boolean;
  rel: "prev" | "next";
  children: React.ReactNode;
}) {
  const cls =
    "inline-flex items-center gap-1.5 rounded-lg px-3 py-2 font-medium transition-colors";
  if (disabled) {
    return <span className={`${cls} cursor-not-allowed text-fg-subtle opacity-50`}>{children}</span>;
  }
  return (
    <Link href={href} rel={rel} scroll={false} className={`${cls} text-fg hover:bg-[var(--surface-hover)]`}>
      {children}
    </Link>
  );
}
