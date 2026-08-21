import "server-only";

import { env } from "@/lib/env";

/**
 * The dashboard's client for the bot's control server (cogs/control_server.py).
 *
 * This is the one place the internet-facing dashboard reaches *into* the running
 * bot process rather than reading its databases at rest. Every other bot bridge
 * in lib/bot is a file the bot happens to also read; this one is a live socket,
 * so it is held to a matching standard:
 *
 * * It only ever talks to VIBEY_CONTROL_URL, which is a loopback address — the
 *   control server binds 127.0.0.1 and is unreachable off the Pi.
 * * Every request carries the shared bearer token. Without it configured, the
 *   channel is treated as unavailable rather than attempted.
 * * Every call has a short timeout and turns any failure into a typed
 *   `ControlUnavailable`, so a page renders an offline state instead of hanging
 *   or crashing when the bot is down.
 *
 * Callers must still gate on ownership themselves (lib/auth/owner.ts) — this
 * module is the transport, not the authorisation.
 */

const DEFAULT_URL = "http://127.0.0.1:8765";
const TIMEOUT_MS = 4000;

export class ControlUnavailable extends Error {
  constructor(message = "The bot's control channel isn't reachable right now.") {
    super(message);
    this.name = "ControlUnavailable";
  }
}

export interface BotHealth {
  status: "online" | "starting";
  user: string | null;
  uptimeSeconds: number;
  latencyMs: number | null;
  guilds: number;
  members: number;
  shards: number;
  loadedCogs: number;
  pid: number;
  discordPy: string;
  python: string;
}

export interface CogInfo {
  name: string;
  module: string;
  loaded: boolean;
  isPackage: boolean;
}

export type CogAction = "load" | "unload" | "reload";
export type PowerAction = "restart" | "reload_all";

export interface CogActionResult {
  ok: boolean;
  /** A full Python traceback on failure — shown inline so it's debuggable. */
  error?: string;
  note?: string;
}

export interface ReloadAllResult {
  ok: boolean;
  reloaded: string[];
  failed: Record<string, string>;
}

export interface LogTail {
  lines: string[];
  available: boolean;
}

/** Whether the control channel is even configured. Cheap; no network. */
export function controlConfigured(): boolean {
  return Boolean(env.VIBEY_CONTROL_TOKEN);
}

function baseUrl(): string {
  return (env.VIBEY_CONTROL_URL ?? DEFAULT_URL).replace(/\/$/, "");
}

async function call<T>(
  path: string,
  init?: { method?: string; body?: unknown },
): Promise<T> {
  if (!env.VIBEY_CONTROL_TOKEN) throw new ControlUnavailable("Control channel not configured.");

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const response = await fetch(`${baseUrl()}${path}`, {
      method: init?.method ?? "GET",
      headers: {
        Authorization: `Bearer ${env.VIBEY_CONTROL_TOKEN}`,
        ...(init?.body ? { "Content-Type": "application/json" } : {}),
      },
      body: init?.body ? JSON.stringify(init.body) : undefined,
      signal: controller.signal,
      // The bot's live state is never cacheable — always ask.
      cache: "no-store",
    });

    // 401 means the two tokens disagree: a config error worth its own message
    // rather than a generic "unavailable".
    if (response.status === 401) {
      throw new ControlUnavailable(
        "The control token is rejected — the dashboard and bot tokens don't match.",
      );
    }

    // The action endpoints return a structured body even on 4xx/5xx (an invalid
    // cog name, a reload traceback), so those are surfaced to the caller as data
    // rather than thrown. Only a truly empty/garbled response is a failure.
    const text = await response.text();
    try {
      return JSON.parse(text) as T;
    } catch {
      throw new ControlUnavailable();
    }
  } catch (error) {
    if (error instanceof ControlUnavailable) throw error;
    // AbortError (timeout), ECONNREFUSED (bot down), DNS, etc.
    throw new ControlUnavailable();
  } finally {
    clearTimeout(timer);
  }
}

/** Live health, or null when the bot can't be reached. */
export async function getHealth(): Promise<BotHealth | null> {
  try {
    return await call<BotHealth>("/health");
  } catch {
    return null;
  }
}

/** The cog list, or null when the bot can't be reached. */
export async function listCogs(): Promise<CogInfo[] | null> {
  try {
    const data = await call<{ cogs: CogInfo[] }>("/cogs");
    return data.cogs;
  } catch {
    return null;
  }
}

/** Perform a cog action. Throws ControlUnavailable if the bot is unreachable. */
export async function cogAction(name: string, action: CogAction): Promise<CogActionResult> {
  return call<CogActionResult>("/cogs", { method: "POST", body: { name, action } });
}

export async function restartBot(): Promise<{ ok: boolean }> {
  return call<{ ok: boolean }>("/power", { method: "POST", body: { action: "restart" } });
}

export async function reloadAllCogs(): Promise<ReloadAllResult> {
  return call<ReloadAllResult>("/power", { method: "POST", body: { action: "reload_all" } });
}

/** The tail of the bot's log, or null when the bot can't be reached. */
export async function tailLogs(lines = 200): Promise<LogTail | null> {
  try {
    return await call<LogTail>(`/logs?lines=${encodeURIComponent(lines)}`);
  } catch {
    return null;
  }
}
