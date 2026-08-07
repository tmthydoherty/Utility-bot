import { handlers } from "@/auth";

/**
 * Auth.js endpoints: /api/auth/signin, /callback/discord, /signout, /session.
 *
 * Node runtime, not edge — the sign-in callback verifies the guild role
 * through the bot token and the surrounding modules are `server-only`.
 */
export const runtime = "nodejs";

export const { GET, POST } = handlers;
