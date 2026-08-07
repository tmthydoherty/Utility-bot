import NextAuth from "next-auth";

import authConfig, { AUTH_PAGES, REVALIDATE_INTERVAL_MS } from "./auth.config";
import { checkGuildAdmin } from "@/lib/auth/authorize";

/**
 * The Node-side auth setup: this is where a session is actually granted.
 *
 * Two rules shape everything below.
 *
 *  1. Authorisation is re-derived from Discord, never trusted from the cookie.
 *     The cookie says who you are; the bot token says what you may do.
 *  2. The user's own OAuth access token is deliberately never written to the
 *     token, so there is nothing in the session worth stealing beyond a user
 *     ID — a leaked cookie cannot be replayed against Discord's API.
 */

/**
 * How long a session survives Discord being unreachable.
 *
 * Strict fail-closed on every error would sign every admin out during a
 * thirty-second Discord blip, which trains people to expect random logouts.
 * An explicit "you are not an admin" still revokes instantly; only "we could
 * not ask" gets this grace, and only if the last *successful* check is recent.
 */
const UNAVAILABLE_GRACE_MS = 30 * 60 * 1000;

export const { handlers, auth, signIn, signOut } = NextAuth({
  ...authConfig,

  callbacks: {
    ...authConfig.callbacks,

    /**
     * Gate the sign-in itself. Returning a path here means the user is bounced
     * before any session cookie is written, so a rejected account leaves no
     * trace of a login at all.
     */
    async signIn({ user }) {
      if (!user.id) return false;
      const result = await checkGuildAdmin(user.id);
      if (result.ok) return true;
      return `${AUTH_PAGES.denied}?reason=${result.reason}`;
    },

    async jwt({ token, user }) {
      // First call after a successful sign-in.
      if (user?.id) {
        token.uid = user.id;
        token.checkedAt = 0; // force an immediate authoritative check below
      }

      if (!token.uid) return token;

      const age = Date.now() - (token.checkedAt ?? 0);
      if (age < REVALIDATE_INTERVAL_MS) return token;

      const result = await checkGuildAdmin(token.uid);

      if (result.ok) {
        token.grantedBy = result.grantedBy;
        token.denied = null;
        token.checkedAt = Date.now();
        return token;
      }

      if (result.reason === "unavailable") {
        const lastGood = token.checkedAt ?? 0;
        if (lastGood > 0 && Date.now() - lastGood < UNAVAILABLE_GRACE_MS) {
          // Leave checkedAt alone so the next request tries again rather than
          // waiting out another full revalidation interval.
          return token;
        }
      }

      // Marked rather than deleted: middleware refuses it, and the UI can say
      // *why* access ended instead of dumping the user at a blank login page.
      token.denied = result.reason;
      token.grantedBy = null;
      token.checkedAt = Date.now();
      return token;
    },

    // `session` is inherited from authConfig on purpose — middleware needs the
    // identical mapping, and two copies would eventually disagree.
  },
});

/**
 * The session for the current request, or null if there isn't a usable one.
 *
 * Every server component and action should go through this rather than `auth()`
 * directly — it is the single spot that treats a denied session as no session,
 * so a page can never accidentally render for a demoted admin by forgetting
 * one boolean.
 */
export async function getActiveSession() {
  const session = await auth();
  if (!session?.user?.id || session.user.denied) return null;
  return session;
}
