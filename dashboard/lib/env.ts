import "server-only";

import { z } from "zod";

/**
 * Environment validated once, at first import, on the server only.
 *
 * A dashboard that boots with a missing bot token and only discovers it when
 * an admin clicks something is worse than one that refuses to start, so every
 * secret is required up front and the failure names the variable.
 */

const snowflake = z
  .string()
  .regex(/^\d{17,20}$/, "must be a Discord snowflake (17-20 digits)");

const schema = z.object({
  DISCORD_CLIENT_ID: z.string().min(1, "DISCORD_CLIENT_ID is required"),
  DISCORD_CLIENT_SECRET: z.string().min(1, "DISCORD_CLIENT_SECRET is required"),
  // Used for *authorisation* checks. The user's own OAuth token is discarded
  // after sign-in; role lookups always go through the bot.
  DISCORD_BOT_TOKEN: z.string().min(1, "DISCORD_BOT_TOKEN is required"),

  // Auth.js signs and encrypts the session cookie with this. 32+ bytes.
  AUTH_SECRET: z
    .string()
    .min(32, "AUTH_SECRET must be at least 32 characters (openssl rand -base64 32)"),
  AUTH_URL: z.string().url().optional(),

  VIBEY_GUILD_ID: snowflake,
  VIBEY_ADMIN_ROLE_ID: snowflake,

  NODE_ENV: z.enum(["development", "production", "test"]).default("development"),
});

function load() {
  // Each variable is named explicitly rather than handing over `process.env`
  // wholesale: the middleware runs on the edge runtime, where `process.env` is
  // not a real object and only statically-referenced keys get inlined at build
  // time. Spreading it there yields an empty object and every check fails.
  const parsed = schema.safeParse({
    DISCORD_CLIENT_ID: process.env.DISCORD_CLIENT_ID,
    DISCORD_CLIENT_SECRET: process.env.DISCORD_CLIENT_SECRET,
    DISCORD_BOT_TOKEN: process.env.DISCORD_BOT_TOKEN,
    AUTH_SECRET: process.env.AUTH_SECRET,
    AUTH_URL: process.env.AUTH_URL,
    VIBEY_GUILD_ID: process.env.VIBEY_GUILD_ID,
    VIBEY_ADMIN_ROLE_ID: process.env.VIBEY_ADMIN_ROLE_ID,
    NODE_ENV: process.env.NODE_ENV,
  });
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((i) => `  - ${i.path.join(".") || "(root)"}: ${i.message}`)
      .join("\n");
    throw new Error(
      `Invalid dashboard environment.\n${issues}\n\n` +
        `Copy dashboard/.env.example to dashboard/.env.local and fill it in.`,
    );
  }
  return parsed.data;
}

export const env = load();

/** True when running behind the tunnel rather than on localhost. */
export const isProduction = env.NODE_ENV === "production";
