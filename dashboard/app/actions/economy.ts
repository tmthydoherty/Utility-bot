"use server";

import { getActiveSession } from "@/auth";
import { env } from "@/lib/env";
import { readUserEconomy, type UserEconomy } from "@/lib/bot/economy-stats";
import { resolveUsers } from "@/lib/bot/directory";

/**
 * The member inspector's one read.
 *
 * A server action so the lookup stays server-side — economy.db is never exposed
 * to the browser — and re-authenticates like every other action here: a request
 * naming another guild, or arriving without a live admin session, gets nothing.
 * It only reads; there is no economy write action, because the inspector is a
 * viewer and the settings tabs already own writing through `saveModuleSettings`.
 */

export interface MemberEconomy extends UserEconomy {
  name: string;
  avatar: string;
  /** True when the member has since left the server. */
  former?: boolean;
}

export interface MemberEconomyResult {
  ok: boolean;
  error?: string;
  data?: MemberEconomy;
}

export async function lookupMemberEconomy(
  guildId: string,
  userId: string,
): Promise<MemberEconomyResult> {
  const session = await getActiveSession();
  if (!session) return { ok: false, error: "Your session has expired. Sign in again." };
  if (guildId !== env.VIBEY_GUILD_ID) return { ok: false, error: "Unknown server." };
  if (!/^\d{17,20}$/.test(userId)) return { ok: false, error: "That isn't a member." };

  const detail = readUserEconomy(userId);
  if (!detail) {
    return {
      ok: false,
      error: "Vibey isn't running, so its economy data can't be read right now.",
    };
  }

  const resolved = (await resolveUsers(guildId, [userId])).get(userId);
  return {
    ok: true,
    data: {
      ...detail,
      name: resolved?.name ?? "Member",
      avatar: resolved?.avatar ?? "",
      former: resolved?.former,
    },
  };
}
