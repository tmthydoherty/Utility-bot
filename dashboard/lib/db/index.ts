import "server-only";

import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";

/**
 * The dashboard's own SQLite file.
 *
 * Deliberately separate from every database the bot owns. Nothing here is
 * authoritative bot state — it holds the audit trail, the rate limiter, and
 * settings drafts — so the bot can never be corrupted or locked by a web
 * request, and this file can be deleted without consequence.
 */

const DATA_DIR = path.join(process.cwd(), "data");
const DB_PATH = path.join(DATA_DIR, "dashboard.db");

let instance: Database.Database | null = null;

function migrate(db: Database.Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at  INTEGER NOT NULL,
      actor_id    TEXT    NOT NULL,
      actor_name  TEXT    NOT NULL,
      guild_id    TEXT    NOT NULL,
      action      TEXT    NOT NULL,
      target      TEXT,
      changes     TEXT,
      ip          TEXT,
      user_agent  TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log (created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_audit_guild   ON audit_log (guild_id, created_at DESC);

    -- One row per (bucket, key) window. Cheaper to prune than to keep a row
    -- per request, and it survives a restart, which an in-memory limiter
    -- would not.
    CREATE TABLE IF NOT EXISTS rate_limit (
      bucket      TEXT    NOT NULL,
      key         TEXT    NOT NULL,
      window_start INTEGER NOT NULL,
      count       INTEGER NOT NULL,
      PRIMARY KEY (bucket, key)
    );

    -- Settings the dashboard has accepted but has not (yet) pushed to the bot.
    -- Until the cog adapters are wired up this is where every save lands.
    CREATE TABLE IF NOT EXISTS settings_draft (
      guild_id    TEXT    NOT NULL,
      module_id   TEXT    NOT NULL,
      data        TEXT    NOT NULL,
      updated_at  INTEGER NOT NULL,
      updated_by  TEXT    NOT NULL,
      PRIMARY KEY (guild_id, module_id)
    );

    -- Module enable/disable, also draft-only for now.
    CREATE TABLE IF NOT EXISTS module_state (
      guild_id    TEXT    NOT NULL,
      module_id   TEXT    NOT NULL,
      enabled     INTEGER NOT NULL,
      updated_at  INTEGER NOT NULL,
      PRIMARY KEY (guild_id, module_id)
    );
  `);
}

export function getDb(): Database.Database {
  if (instance) return instance;

  fs.mkdirSync(DATA_DIR, { recursive: true });

  const db = new Database(DB_PATH);
  // WAL so a long read (the audit page) never blocks a write, and a 5s busy
  // timeout so two concurrent requests queue instead of throwing SQLITE_BUSY.
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("busy_timeout = 5000");
  db.pragma("foreign_keys = ON");

  migrate(db);

  instance = db;
  return db;
}
