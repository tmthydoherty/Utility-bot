import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { listAuditFiltered } from "@/lib/db/audit";
import { describeAction } from "@/lib/audit/labels";

/**
 * CSV export of the audit log.
 *
 * A route handler rather than a server action because the browser needs to
 * *download* the result, not receive it in a fetch. It re-authenticates and
 * re-pins the guild like every other entry point here — a download URL is just
 * another public endpoint — and honours the same `actor`/`category` filters the
 * page shows, so "export" means "export what I'm looking at".
 */

function csvCell(value: unknown): string {
  const s = value == null ? "" : String(value);
  // Quote always; double any embedded quotes. Keeps commas, newlines and the
  // occasional leading `=` (which spreadsheets would treat as a formula) inert.
  return `"${s.replace(/"/g, '""')}"`;
}

function renderChanges(changes: Record<string, { from: unknown; to: unknown }> | null): string {
  if (!changes) return "";
  return Object.entries(changes)
    .map(([key, { from, to }]) => `${key}: ${JSON.stringify(from)} → ${JSON.stringify(to)}`)
    .join("; ");
}

export async function GET(
  request: Request,
  { params }: { params: Promise<{ guildId: string }> },
) {
  const session = await getActiveSession();
  if (!session) return new Response("Unauthorized", { status: 401 });

  const { guildId } = await params;
  if (guildId !== env.VIBEY_GUILD_ID) return new Response("Unknown server", { status: 404 });

  const url = new URL(request.url);
  const actorId = url.searchParams.get("actor");
  const category = url.searchParams.get("category");

  // Cap the export so a runaway log can't stream unbounded; 10k rows is far more
  // than anyone reviews and still a small file.
  const entries = listAuditFiltered(guildId, { actorId, category, limit: 10_000, offset: 0 });

  const header = ["When (UTC)", "Who", "Action", "Target", "Changes", "IP"];
  const lines = [
    header.map(csvCell).join(","),
    ...entries.map((e) =>
      [
        new Date(e.createdAt).toISOString(),
        e.actorName,
        describeAction(e.action),
        e.target ?? "",
        renderChanges(e.changes),
        e.ip ?? "",
      ]
        .map(csvCell)
        .join(","),
    ),
  ];
  // A BOM so Excel opens UTF-8 names correctly.
  const body = `﻿${lines.join("\r\n")}\r\n`;

  const stamp = new Date().toISOString().slice(0, 10);
  return new Response(body, {
    headers: {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="vibey-audit-${stamp}.csv"`,
      "Cache-Control": "no-store",
    },
  });
}
