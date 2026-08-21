import "server-only";

import { env } from "@/lib/env";

/**
 * Who counts as the bot owner.
 *
 * The rest of the dashboard authorises any server admin (see authorize.ts). The
 * Bot Control section is stricter: only the single account named in
 * VIBEY_OWNER_ID may reach it, because reloading the bot's code or restarting
 * the process is an operator action with no moderator equivalent.
 *
 * Fails closed. If VIBEY_OWNER_ID isn't configured there is no owner, so this
 * returns false for everyone rather than defaulting the door open — a control
 * channel that anyone with admin could drive would defeat the point of the tier.
 */
export function isOwnerId(userId: string | null | undefined): boolean {
  if (!env.VIBEY_OWNER_ID) return false;
  if (!userId) return false;
  return userId === env.VIBEY_OWNER_ID;
}
