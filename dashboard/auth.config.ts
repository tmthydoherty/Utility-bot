import type { NextAuthConfig } from "next-auth";
import Discord from "next-auth/providers/discord";

/**
 * The edge-safe half of the auth setup.
 *
 * middleware.ts runs on the edge runtime and cannot pull in `server-only`,
 * better-sqlite3, or anything else Node-specific — so it gets this config,
 * which knows how to *decode and validate* a session but not how to *grant*
 * one. The authorisation callbacks that talk to Discord live in auth.ts,
 * which only ever runs in a Node context (pages, route handlers, actions).
 */

export const AUTH_PAGES = {
  signIn: "/login",
  error: "/auth/error",
  denied: "/auth/denied",
} as const;

/** How long a session survives without any activity. */
export const SESSION_MAX_AGE = 8 * 60 * 60; // 8 hours

/** How often the JWT is rewritten, which is also how often we re-check roles. */
export const SESSION_UPDATE_AGE = 15 * 60; // 15 minutes

/**
 * Upper bound on how long a demoted admin keeps access.
 *
 * Waiting for the 8-hour cookie to lapse would mean someone stripped of the
 * admin role could keep changing settings for the rest of the day. Five
 * minutes is short enough to be a real revocation and long enough that normal
 * browsing doesn't hammer Discord.
 */
export const REVALIDATE_INTERVAL_MS = 5 * 60 * 1000;

export const authConfig = {
  providers: [
    Discord({
      clientId: process.env.DISCORD_CLIENT_ID,
      clientSecret: process.env.DISCORD_CLIENT_SECRET,
      // `identify` only. The dashboard never acts as the user against Discord —
      // every lookup goes through the bot token — so a broader scope would be
      // permission we hold without ever using.
      authorization: { params: { scope: "identify" } },
    }),
  ],

  pages: {
    signIn: AUTH_PAGES.signIn,
    error: AUTH_PAGES.error,
  },

  session: {
    strategy: "jwt",
    maxAge: SESSION_MAX_AGE,
    updateAge: SESSION_UPDATE_AGE,
  },

  cookies: {
    sessionToken: {
      name:
        process.env.NODE_ENV === "production"
          ? "__Secure-vibey.session"
          : "vibey.session",
      options: {
        httpOnly: true,
        // Lax rather than Strict: Strict would drop the cookie on the return
        // leg of the Discord OAuth redirect, so sign-in would silently loop.
        sameSite: "lax",
        path: "/",
        secure: process.env.NODE_ENV === "production",
      },
    },
  },

  // The tunnel terminates TLS at Cloudflare and forwards to localhost, so the
  // Host header is the only way Auth.js can know its own public origin.
  trustHost: true,

  callbacks: {
    /**
     * Copy our claims off the token onto the session.
     *
     * This lives in the shared config rather than in auth.ts because both
     * NextAuth instances — the Node one and the edge one middleware builds —
     * have to agree on what a session looks like. When it was only in auth.ts,
     * middleware saw a session with no `id` and no `denied`, judged every
     * authenticated request unauthorised, and bounced it to /login, which
     * bounced it back: an infinite redirect for anyone who successfully
     * signed in.
     */
    session({ session, token }) {
      if (token.uid) session.user.id = token.uid;
      session.user.grantedBy = token.grantedBy ?? null;
      session.user.denied = token.denied ?? null;
      session.user.checkedAt = token.checkedAt ?? 0;
      return session;
    },

    /**
     * Used by middleware. A decoded token is not enough on its own — a token
     * whose last role check failed is kept around only so the UI can explain
     * why, and must not open any door.
     */
    authorized({ auth }) {
      return Boolean(auth?.user?.id && !auth.user.denied);
    },
  },
} satisfies NextAuthConfig;

export default authConfig;
