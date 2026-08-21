"use server";

import { headers } from "next/headers";
import { revalidatePath } from "next/cache";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { diffValues, recordAudit } from "@/lib/db/audit";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";
import {
  saveGuildConfig,
  getGuildConfig,
  createShuffleTask,
  deleteShuffleTask,
} from "@/lib/roles-and-alerts/store";

/**
 * Roles & Alerts mutations.
 *
 * These write role configuration — which roles gate which tier, which a shuffle
 * grants and strips — so, like every other action here, each one re-authenticates,
 * pins the guild, rate-limits, and records what moved before returning. Role
 * changes are the one thing on this dashboard worth being able to reconstruct
 * after the fact, so the audit trail matters more here than almost anywhere.
 * The callers wrap these in try/catch and toast on failure, so throwing on a
 * rejected request is the right contract.
 */

async function guard(guildId: string) {
  const session = await getActiveSession();
  if (!session) throw new Error("unauthorized");

  // The session proves admin in *our* guild; a request naming another is a bug
  // or a probe.
  if (guildId !== env.VIBEY_GUILD_ID) throw new Error("unknown server");

  const limit = checkRateLimit("mutation", session.user.id);
  if (!limit.allowed) throw new Error("rate limited");

  const headerList = await headers();
  return {
    session,
    ip: clientIp(headerList),
    userAgent: headerList.get("user-agent") ?? "",
  };
}

export async function saveRolesAndAlertsSettings(
  guildId: string,
  updates: {
    channel_id?: string | null;
    banners?: { alerts: string | null; colors: string | null };
    gates?: { t2: string | null; t3: string | null };
    vip_roles?: { t2: string[]; t3: string[] };
    colors?: { t1: string[]; t2: string[]; t3: string[] };
    alert_roles?: string[];
  },
) {
  const { session, ip, userAgent } = await guard(guildId);

  // Read a full snapshot before mutating — getGuildConfig re-parses from disk,
  // so this is an independent object, not a live reference the save would edit
  // underneath us.
  const before = getGuildConfig(guildId);

  saveGuildConfig(guildId, (cfg) => {
    if (updates.channel_id !== undefined) cfg.channel_id = updates.channel_id;
    if (updates.banners !== undefined) cfg.banners = updates.banners;
    if (updates.gates !== undefined) {
      if (updates.gates.t2 !== undefined) cfg.gates.t2 = updates.gates.t2;
      if (updates.gates.t3 !== undefined) cfg.gates.t3 = updates.gates.t3;
    }
    if (updates.vip_roles !== undefined) cfg.vip_roles = updates.vip_roles;
    if (updates.colors !== undefined) cfg.colors = updates.colors;
    if (updates.alert_roles !== undefined) cfg.alert_roles = updates.alert_roles;
  });

  const after = getGuildConfig(guildId);
  // Diff the whole config: fields the bot owns (messages, posted banners) don't
  // move on a settings save, so only what the admin actually changed is logged.
  const changes = diffValues(
    before as unknown as Record<string, unknown>,
    after as unknown as Record<string, unknown>,
  );

  if (Object.keys(changes).length > 0) {
    recordAudit({
      actorId: session.user.id,
      actorName: session.user.name ?? session.user.id,
      guildId,
      action: "roles-and-alerts.settings.update",
      target: guildId,
      changes,
      ip,
      userAgent,
    });
  }

  revalidatePath(`/dashboard/${guildId}/modules/roles-and-alerts`);
}

export async function submitRoleShuffle(
  guildId: string,
  triggerRoles: string[],
  gainRole: string,
  loseRole: string | null,
) {
  const { session, ip, userAgent } = await guard(guildId);
  if (!triggerRoles.length) throw new Error("Must select at least one trigger role");
  if (!gainRole) throw new Error("Must select a gain role");

  createShuffleTask(guildId, triggerRoles, gainRole, loseRole);

  recordAudit({
    actorId: session.user.id,
    actorName: session.user.name ?? session.user.id,
    guildId,
    action: "roles-and-alerts.shuffle.create",
    target: gainRole,
    changes: {
      trigger_roles: { from: null, to: triggerRoles },
      gain_role: { from: null, to: gainRole },
      lose_role: { from: null, to: loseRole },
    },
    ip,
    userAgent,
  });

  revalidatePath(`/dashboard/${guildId}/modules/roles-and-alerts`);
}

export async function removeRoleShuffle(guildId: string, id: string) {
  const { session, ip, userAgent } = await guard(guildId);

  deleteShuffleTask(id);

  recordAudit({
    actorId: session.user.id,
    actorName: session.user.name ?? session.user.id,
    guildId,
    action: "roles-and-alerts.shuffle.delete",
    target: id,
    ip,
    userAgent,
  });

  revalidatePath(`/dashboard/${guildId}/modules/roles-and-alerts`);
}
