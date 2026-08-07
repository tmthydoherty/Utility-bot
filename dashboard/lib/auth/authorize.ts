import "server-only";

import { env } from "@/lib/env";
import { getGuild, getMember, getRoles } from "@/lib/discord/rest";
import { Permissions } from "@/lib/discord/types";

/**
 * The one place that decides who may use the dashboard.
 *
 * This mirrors `Vibey.is_bot_admin` in main.py exactly — Administrator
 * permission or the admin role — so the website can never grant access the
 * bot's own slash commands would refuse. If that rule changes in main.py, it
 * changes here too.
 */

export type DenyReason = "not-member" | "no-admin" | "unavailable";

export type AuthorizationResult =
  | { ok: true; grantedBy: "owner" | "admin-role" | "administrator" }
  | { ok: false; reason: DenyReason };

export const DENY_MESSAGES: Record<DenyReason, string> = {
  "not-member": "You're not a member of the Vibey server.",
  "no-admin": "Your account doesn't have administrator access in the Vibey server.",
  unavailable: "Couldn't reach Discord to verify your access. Try again in a moment.",
};

export async function checkGuildAdmin(userId: string): Promise<AuthorizationResult> {
  try {
    const guildId = env.VIBEY_GUILD_ID;

    const [guild, member] = await Promise.all([getGuild(guildId), getMember(guildId, userId)]);

    if (guild.owner_id === userId) return { ok: true, grantedBy: "owner" };

    // A 404 from the members endpoint is an answer, not a failure: they simply
    // aren't in the server.
    if (!member) return { ok: false, reason: "not-member" };

    if (member.roles.includes(env.VIBEY_ADMIN_ROLE_ID)) {
      return { ok: true, grantedBy: "admin-role" };
    }

    // Discord only sends role IDs on a member, so Administrator has to be
    // resolved against the guild's role list rather than read off the member.
    const roles = await getRoles(guildId);
    const held = new Set(member.roles);
    const isAdministrator = roles.some(
      (role) => held.has(role.id) && (BigInt(role.permissions) & Permissions.ADMINISTRATOR) !== 0n,
    );

    if (isAdministrator) return { ok: true, grantedBy: "administrator" };

    return { ok: false, reason: "no-admin" };
  } catch (error) {
    // Fail closed. A Discord outage locking admins out for a few minutes is a
    // far better failure than an outage letting anyone in — and every other
    // branch above returns an explicit decision, so reaching here really does
    // mean "we don't know".
    //
    // It must also say *why*. This catch was silent to begin with, and the
    // first time it fired in production the only evidence anywhere was a
    // user-facing "couldn't reach Discord" — no stack, no status, nothing in
    // the journal. An authorisation path that fails invisibly is one you
    // cannot debug at exactly the moment you need to.
    console.error(
      `[authorize] guild admin check failed for user ${userId}:`,
      error instanceof Error ? `${error.name}: ${error.message}` : error,
      error instanceof Error && error.stack ? `\n${error.stack}` : "",
    );
    return { ok: false, reason: "unavailable" };
  }
}
