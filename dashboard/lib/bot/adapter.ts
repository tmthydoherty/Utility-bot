import "server-only";

import { getDb } from "@/lib/db";
import { defaultsFor, type SettingsValues } from "@/lib/schema/types";
import { getModule } from "@/lib/schema/modules";

/**
 * The seam between the dashboard and the bot.
 *
 * Everything the dashboard will eventually read from or write to Vibey goes
 * through this one interface. Right now it is backed entirely by the
 * dashboard's own SQLite file: reads fall back to schema defaults, and writes
 * land in a draft table. Nothing here touches the bot's JSON configs, its
 * databases, or Discord.
 *
 * That is the point. When the cogs are wired up, the change is this file
 * choosing a different backing store — direct reads of the bot's config files
 * via a Python-side API, most likely — and no page, form or component above it
 * has to change. Keeping the boundary honest now is what stops the wiring
 * later from turning into a rewrite.
 */

export interface ModuleState {
  moduleId: string;
  enabled: boolean;
  updatedAt: number | null;
}

/** Overview numbers. Placeholder shapes, real shapes — see note in stats(). */
export interface GuildStats {
  members: number;
  online: number;
  messages7d: number;
  messagesTrend: number[];
  voiceMinutes7d: number;
  newMembers7d: number;
  activeModules: number;
  totalModules: number;
}

interface DraftRow {
  data: string;
  updated_at: number;
  updated_by: string;
}

export function readSettings(guildId: string, moduleId: string): SettingsValues {
  const moduleSchema = getModule(moduleId);
  if (!moduleSchema) return {};

  const base = defaultsFor(moduleSchema);

  const row = getDb()
    .prepare(`SELECT data, updated_at, updated_by FROM settings_draft WHERE guild_id = ? AND module_id = ?`)
    .get(guildId, moduleId) as DraftRow | undefined;

  if (!row) return base;

  try {
    // Spread over defaults rather than replacing them: a schema that gained a
    // field since the draft was written must not read back as undefined.
    return { ...base, ...(JSON.parse(row.data) as SettingsValues) };
  } catch {
    return base;
  }
}

export function readSettingsMeta(
  guildId: string,
  moduleId: string,
): { updatedAt: number; updatedBy: string } | null {
  const row = getDb()
    .prepare(`SELECT data, updated_at, updated_by FROM settings_draft WHERE guild_id = ? AND module_id = ?`)
    .get(guildId, moduleId) as DraftRow | undefined;
  return row ? { updatedAt: row.updated_at, updatedBy: row.updated_by } : null;
}

export function writeSettings(
  guildId: string,
  moduleId: string,
  values: SettingsValues,
  actorId: string,
): void {
  getDb()
    .prepare(
      `INSERT INTO settings_draft (guild_id, module_id, data, updated_at, updated_by)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (guild_id, module_id) DO UPDATE SET
         data = excluded.data, updated_at = excluded.updated_at, updated_by = excluded.updated_by`,
    )
    .run(guildId, moduleId, JSON.stringify(values), Date.now(), actorId);
}

export function readModuleStates(guildId: string): Map<string, ModuleState> {
  const rows = getDb()
    .prepare(`SELECT module_id, enabled, updated_at FROM module_state WHERE guild_id = ?`)
    .all(guildId) as { module_id: string; enabled: number; updated_at: number }[];

  return new Map(
    rows.map((row) => [
      row.module_id,
      { moduleId: row.module_id, enabled: row.enabled === 1, updatedAt: row.updated_at },
    ]),
  );
}

export function setModuleEnabled(guildId: string, moduleId: string, enabled: boolean): void {
  getDb()
    .prepare(
      `INSERT INTO module_state (guild_id, module_id, enabled, updated_at)
       VALUES (?, ?, ?, ?)
       ON CONFLICT (guild_id, module_id) DO UPDATE SET
         enabled = excluded.enabled, updated_at = excluded.updated_at`,
    )
    .run(guildId, moduleId, enabled ? 1 : 0, Date.now());
}

/**
 * Overview statistics.
 *
 * The member and presence counts are real — Discord returns them with the
 * guild. Everything below them is placeholder: the bot already records this in
 * tracking_data.db, but reading it is out of scope for this pass, and inventing
 * a plausible-looking number is better than shipping an empty page as long as
 * the UI says so out loud. Every caller renders these behind a "sample data"
 * marker.
 */
export function stats(
  guildId: string,
  memberCount: number,
  presenceCount: number,
  moduleCounts: { active: number; total: number },
): GuildStats {
  // Seeded off the guild ID so the numbers are stable across renders instead
  // of flickering on every request.
  const seed = Number(BigInt(guildId) % 997n);
  const trend = Array.from({ length: 14 }, (_, i) => {
    const wave = Math.sin((i + seed) / 2.4) * 0.28 + Math.sin((i + seed) / 5.1) * 0.16;
    return Math.max(120, Math.round(900 * (1 + wave)));
  });

  return {
    members: memberCount,
    online: presenceCount,
    messages7d: trend.slice(-7).reduce((a, b) => a + b, 0),
    messagesTrend: trend,
    voiceMinutes7d: 4200 + (seed % 900),
    newMembers7d: 12 + (seed % 20),
    activeModules: moduleCounts.active,
    totalModules: moduleCounts.total,
  };
}

/** True while the numbers above are not yet coming from the bot. */
export const STATS_ARE_SAMPLE = true;
