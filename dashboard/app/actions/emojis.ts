"use server";

import { getActiveSession } from "@/auth";
import { checkGuildAdmin } from "@/lib/auth/authorize";
import { loadCrossServerEmojis, primaryGuildId } from "@/lib/guild";
import type { GuildEmoji } from "@/components/providers/guild-provider";

/**
 * The cross-server custom-emoji list, loaded on demand.
 *
 * This used to be gathered inside `loadGuild` and handed to every page through
 * the guild provider, which meant the dashboard layout waited on one REST call
 * per server the bot is in before rendering anything. Only the automations
 * button picker ever uses it, so it's pulled here instead — after the page is
 * already interactive — and just for that one field.
 *
 * It re-authenticates and re-authorises like every other action: a server
 * action is a public endpoint, so the button that calls it proves nothing. On
 * any failure it returns an empty list, and the picker falls back to this
 * server's emoji (already in the provider) rather than erroring.
 */
export async function fetchCrossServerEmojis(): Promise<GuildEmoji[]> {
  const session = await getActiveSession();
  if (!session) return [];

  const auth = await checkGuildAdmin(session.user.id);
  if (!auth.ok) return [];

  try {
    return await loadCrossServerEmojis(primaryGuildId());
  } catch {
    return [];
  }
}
