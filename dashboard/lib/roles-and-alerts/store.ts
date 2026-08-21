import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

const CONFIG_DIR = path.join(process.cwd(), "..", "data", "roles_and_alerts_config");
const CONFIG_PATH = path.join(CONFIG_DIR, "config.json");
const DB_PATH = path.join(CONFIG_DIR, "commands.db");

// --- JSON Config ---

export interface RolesAndAlertsConfig {
  channel_id: string | null;
  alert_roles: string[];
  gates: {
    t1: string | null;
    t2: string | null;
    t3: string | null;
  };
  colors: {
    t1: string[];
    t2: string[];
    t3: string[];
  };
  messages: Record<string, string>;
  last_colors: Record<string, Record<string, string>>;
  banners: {
    alerts: string | null;
    colors: string | null;
  };
  posted_banners: Record<string, string>;
  vip_roles: {
    t2: string[];
    t3: string[];
  };
}

function getEmptyConfig(): RolesAndAlertsConfig {
  return {
    channel_id: null,
    alert_roles: [],
    gates: { t1: null, t2: null, t3: null },
    colors: { t1: [], t2: [], t3: [] },
    messages: {},
    last_colors: {},
    banners: { alerts: null, colors: null },
    posted_banners: {},
    vip_roles: { t2: [], t3: [] },
  };
}

export function readConfig(): Record<string, RolesAndAlertsConfig> {
  if (!fs.existsSync(CONFIG_PATH)) {
    return {};
  }
  try {
    const raw = fs.readFileSync(CONFIG_PATH, "utf-8");
    // Discord IDs are up to 20 digits; prevent precision loss
    const safeRaw = raw.replace(/(?<!["\w])\b\d{16,20}\b(?!["\w])/g, '"$&"');
    return JSON.parse(safeRaw);
  } catch {
    return {};
  }
}

export function writeConfig(data: Record<string, RolesAndAlertsConfig>) {
  if (!fs.existsSync(CONFIG_DIR)) {
    fs.mkdirSync(CONFIG_DIR, { recursive: true });
  }
  const tmp = `${CONFIG_PATH}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), "utf-8");
  fs.renameSync(tmp, CONFIG_PATH);
}

export function getGuildConfig(guildId: string): RolesAndAlertsConfig {
  const data = readConfig();
  if (data[guildId]) return data[guildId];
  return getEmptyConfig();
}

export function saveGuildConfig(guildId: string, mutator: (cfg: RolesAndAlertsConfig) => void) {
  const data = readConfig();
  if (!data[guildId]) {
    data[guildId] = getEmptyConfig();
  }
  mutator(data[guildId]);
  writeConfig(data);
}

// --- SQLite Commands ---

let dbInstance: Database.Database | null = null;

export function openCommandsDb(): Database.Database {
  if (dbInstance) return dbInstance;
  if (!fs.existsSync(CONFIG_DIR)) {
    fs.mkdirSync(CONFIG_DIR, { recursive: true });
  }

  const db = new Database(DB_PATH);
  db.defaultSafeIntegers(true);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("busy_timeout = 5000");

  db.exec(`
    CREATE TABLE IF NOT EXISTS role_shuffle_tasks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      guild_id TEXT NOT NULL,
      trigger_roles TEXT NOT NULL,
      gain_role TEXT NOT NULL,
      lose_role TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      logs TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  dbInstance = db;
  return db;
}

export interface RoleShuffleTask {
  id: string;
  guild_id: string;
  trigger_roles: string[];
  gain_role: string;
  lose_role: string | null;
  status: "pending" | "running" | "complete" | "error";
  logs: string | null;
  created_at: string;
}

export function listShuffleTasks(guildId: string): RoleShuffleTask[] {
  const db = openCommandsDb();
  const rows = db.prepare(`SELECT * FROM role_shuffle_tasks WHERE guild_id = ? ORDER BY id DESC LIMIT 50`).all(guildId) as Record<string, unknown>[];
  
  return rows.map((row) => ({
    id: String(row.id),
    guild_id: String(row.guild_id),
    trigger_roles: JSON.parse(String(row.trigger_roles)),
    gain_role: String(row.gain_role),
    lose_role: row.lose_role ? String(row.lose_role) : null,
    status: String(row.status) as RoleShuffleTask["status"],
    logs: row.logs ? String(row.logs) : null,
    created_at: String(row.created_at),
  }));
}

export function createShuffleTask(guildId: string, triggerRoles: string[], gainRole: string, loseRole: string | null) {
  const db = openCommandsDb();
  db.prepare(`
    INSERT INTO role_shuffle_tasks (guild_id, trigger_roles, gain_role, lose_role)
    VALUES (?, ?, ?, ?)
  `).run(guildId, JSON.stringify(triggerRoles), gainRole, loseRole);
}

export function deleteShuffleTask(id: string) {
  const db = openCommandsDb();
  db.prepare(`DELETE FROM role_shuffle_tasks WHERE id = ?`).run(id);
}
