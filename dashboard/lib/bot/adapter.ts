import "server-only";

import { getDb } from "@/lib/db";
import { defaultsFor, type SettingsValues } from "@/lib/schema/types";
import { getModule } from "@/lib/schema/modules";
import {
  isBotBacked,
  economyStoreAvailable,
  readEconomyModuleValues,
  readEconomyMeta,
  writeEconomyModuleValues,
} from "@/lib/bot/economy-config";
import {
  isModuleSynced,
  moduleConfigStoreAvailable,
  readModuleValues,
  readModuleMeta,
  writeModuleValues,
} from "@/lib/bot/module-config";

export { isBotBacked, EconomyConfigUnavailable } from "@/lib/bot/economy-config";
export { isModuleSynced, ModuleConfigUnavailable } from "@/lib/bot/module-config";

/**
 * A module is "live" when a save here reaches the running bot rather than
 * sitting in the dashboard's draft table — economy and leveling through their
 * own config bridge, welcome/security/suggestions through the shared per-guild
 * one. Everything else still drafts.
 */
export function isLive(moduleId: string): boolean {
  return isBotBacked(moduleId) || isModuleSynced(moduleId);
}

/** Whether the store behind a live module is reachable right now. */
export function storeReachable(moduleId: string): boolean {
  if (isBotBacked(moduleId)) return economyStoreAvailable();
  if (isModuleSynced(moduleId)) return moduleConfigStoreAvailable();
  return false;
}

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

interface DraftRow {
  data: string;
  updated_at: number;
  updated_by: string;
}

export function readSettings(guildId: string, moduleId: string): SettingsValues {
  const moduleSchema = getModule(moduleId);
  if (!moduleSchema) return {};

  // Economy and Leveling read their live values from the bot's config store
  // rather than the draft table — see lib/bot/economy-config.ts.
  if (isBotBacked(moduleId)) return readEconomyModuleValues(moduleSchema);
  // Welcome/Security/Suggestions read from the shared per-guild bridge.
  if (isModuleSynced(moduleId)) return readModuleValues(moduleSchema, guildId);

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
  if (isBotBacked(moduleId)) {
    const moduleSchema = getModule(moduleId);
    const meta = moduleSchema ? readEconomyMeta(moduleSchema) : null;
    return meta ? { updatedAt: meta.updatedAt, updatedBy: "" } : null;
  }
  if (isModuleSynced(moduleId)) {
    const moduleSchema = getModule(moduleId);
    const meta = moduleSchema ? readModuleMeta(moduleSchema, guildId) : null;
    return meta ? { updatedAt: meta.updatedAt, updatedBy: "" } : null;
  }

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
  const moduleSchema = getModule(moduleId);
  if (moduleSchema && isBotBacked(moduleId)) {
    // Throws EconomyConfigUnavailable if the bot has not created the store yet;
    // the caller turns that into a friendly "the bot isn't running" message.
    writeEconomyModuleValues(moduleSchema, values, actorId);
    return;
  }
  if (moduleSchema && isModuleSynced(moduleId)) {
    // Throws ModuleConfigUnavailable if the bridge doesn't exist yet.
    writeModuleValues(moduleSchema, guildId, values, actorId);
    return;
  }

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

// Overview statistics used to live here as seeded placeholder numbers. They now
// come from the bot's own databases, read-only — see lib/bot/tracker-stats.ts
// and lib/bot/tracker-view.ts, which the overview and the Activity Tracker page
// consume directly.
