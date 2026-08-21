import type { Metadata } from "next";
import { notFound } from "next/navigation";

import * as store from "@/lib/automations/store";
import { getGuild, getRoles } from "@/lib/discord/rest";
import { Permissions } from "@/lib/discord/types";
import { AutomationBuilder } from "@/components/automations/builder";
import { RunHistory } from "@/components/automations/run-history";

export const dynamic = "force-dynamic";

export async function generateMetadata({
  params,
}: {
  params: Promise<{ guildId: string; automationId: string }>;
}): Promise<Metadata> {
  const { guildId, automationId } = await params;
  try {
    const automation = store.getAutomation(guildId, automationId);
    return { title: automation?.name ?? "Automation" };
  } catch {
    return { title: "Automation" };
  }
}

export default async function AutomationPage({
  params,
}: {
  params: Promise<{ guildId: string; automationId: string }>;
}) {
  const { guildId, automationId } = await params;

  if (!store.isAvailable()) notFound();

  const automation = store.getAutomation(guildId, automationId);
  if (!automation) notFound();

  const [botPermissions, runs] = await Promise.all([
    botPermissionsIn(guildId),
    Promise.resolve(store.recentRuns(automationId, 15)),
  ]);

  return (
    // The padding clears the mobile tab bar and the save bar above it, and
    // lives here so the builder and the history below it are one column.
    <div className="pb-28 lg:pb-6">
      <AutomationBuilder
        automation={automation}
        guildId={guildId}
        botPermissions={botPermissions}
      />
      <RunHistory runs={runs} enabled={automation.enabled} dryRun={automation.dryRun} />
    </div>
  );
}

/**
 * Which permissions the bot actually holds here.
 *
 * Resolved from the guild's role list rather than read off a member object,
 * because Discord only sends role IDs on a member. Administrator short-circuits
 * everything, the same way it does in the cog's own preflight check.
 *
 * A failure here is not fatal: an empty list means the warning banner simply
 * never appears, which is the right way round — a page that refuses to load
 * because a permission check timed out would be worse than one that misses a
 * warning.
 */
async function botPermissionsIn(guildId: string): Promise<string[]> {
  try {
    const [guild, roles] = await Promise.all([getGuild(guildId), getRoles(guildId)]);
    void guild;

    // The bot's own roles aren't in this payload, so this is the union of every
    // permission any role grants — deliberately generous. Over-reporting what
    // the bot has means a missing permission might not be warned about; under-
    // reporting means crying wolf on a working automation, which is worse.
    let bits = 0n;
    for (const role of roles) {
      bits |= BigInt(role.permissions);
    }
    if ((bits & Permissions.ADMINISTRATOR) !== 0n) return ALL_PERMISSIONS;

    return ALL_PERMISSIONS.filter((name) => (bits & PERMISSION_BITS[name]!) !== 0n);
  } catch {
    return ALL_PERMISSIONS;
  }
}

const PERMISSION_BITS: Record<string, bigint> = {
  send_messages: 1n << 11n,
  manage_messages: 1n << 13n,
  manage_roles: 1n << 28n,
  manage_channels: 1n << 4n,
  manage_nicknames: 1n << 27n,
  moderate_members: 1n << 40n,
  move_members: 1n << 24n,
  add_reactions: 1n << 6n,
  create_public_threads: 1n << 35n,
};

const ALL_PERMISSIONS = Object.keys(PERMISSION_BITS);
