import { notFound, redirect } from "next/navigation";

import { getActiveSession } from "@/auth";
import { isOwnerId } from "@/lib/auth/owner";
import { loadGuild, primaryGuildId } from "@/lib/guild";
import { avatarUrl } from "@/lib/discord/types";
import { AppShell } from "@/components/shell/app-shell";

/**
 * The authenticated layout.
 *
 * Middleware already turns anonymous requests away, but this checks again. The
 * two are not redundant: middleware validates the cookie on the edge, while
 * `getActiveSession` runs the Node-side callbacks that re-verify the role
 * against Discord. A demoted admin has a perfectly valid cookie right up until
 * this call rejects them.
 */
export default async function GuildLayout({
  children,
  params,
}: {
  children: React.ReactNode;
  params: Promise<{ guildId: string }>;
}) {
  const session = await getActiveSession();
  if (!session) redirect("/login");

  const { guildId } = await params;
  // The session authorises exactly one guild; any other ID in the URL is not a
  // server this dashboard knows about.
  if (guildId !== primaryGuildId()) notFound();

  const guild = await loadGuild(guildId);

  return (
    <AppShell
      guild={guild}
      isOwner={isOwnerId(session.user.id)}
      user={{
        name: session.user.name ?? "Admin",
        avatarUrl: session.user.image ?? avatarUrl({ id: session.user.id, avatar: null }),
        grantedBy: session.user.grantedBy,
      }}
    >
      {children}
    </AppShell>
  );
}
