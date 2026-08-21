import { notFound } from "next/navigation";

import { getActiveSession } from "@/auth";
import { isOwnerId } from "@/lib/auth/owner";

/**
 * The gate for the entire Bot Control section.
 *
 * The guild layout above already proved the viewer is an admin of this server;
 * this narrows that to the bot *owner*. Enforcing it here — not just by hiding
 * the nav link — is the actual access control: a non-owner who types the URL,
 * or who never sees the link at all, gets a 404 rather than the page.
 *
 * `notFound()` rather than a redirect: there's nothing to tell a non-owner
 * about a section they aren't meant to know exists.
 */
export default async function ControlLayout({ children }: { children: React.ReactNode }) {
  const session = await getActiveSession();
  if (!isOwnerId(session?.user.id)) notFound();
  return <>{children}</>;
}
