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

  // The bot owner's Discord user ID. This is the *only* account allowed into
  // the Bot Control section — a stricter tier than the admin access the rest of
  // the dashboard grants, because restarting the bot or reloading its code is
  // an operator action, not a moderator one. Optional so the dashboard still
  // boots without it; when unset, nobody is the owner and the whole section
  // stays hidden and refuses every control action (fail closed).
  VIBEY_OWNER_ID: snowflake.optional(),

  // The bot's localhost control server (cogs/control_server.py) and the shared
  // bearer token that authenticates the dashboard to it. The URL defaults to
  // where the cog binds on the Pi; the token must match VIBEY_CONTROL_TOKEN in
  // the bot's own .env. Both optional: without the token the control client
  // reports the channel unavailable and the section renders its offline state.
  VIBEY_CONTROL_URL: z.string().url().optional(),
  VIBEY_CONTROL_TOKEN: z.string().min(1).optional(),

  // The bot's own automations database, which the builder reads and writes
  // directly — see lib/automations/store.ts for why that is a deliberate
  // exception. Optional so a development machine can point at a copy; it
  // defaults to ../automations.db, which is where it sits on the Pi.
  VIBEY_AUTOMATIONS_DB: z.string().min(1).optional(),

  // The bot's Utility database (reminders, one-off messages, stickies, reaction
  // rules, media channels), written directly the same way as automations — see
  // lib/utility/store.ts. Optional; defaults to ../data/utility/utility.db.
  VIBEY_UTILITY_DB: z.string().min(1).optional(),

  // The bot's shared Economy/Leveling config file, which those two modules read
  // and write directly — the same deliberate exception, see lib/bot/economy-
  // config.ts. Optional; defaults to ../data/economy_config/economy_config.db.
  VIBEY_ECONOMY_CONFIG_DB: z.string().min(1).optional(),

  // The bot's shared per-guild module config bridge (welcome, security,
  // suggestions), read and written the same way as the economy bridge — see
  // lib/bot/module-config.ts. Optional; defaults to
  // ../data/module_config/module_config.db.
  VIBEY_MODULE_CONFIG_DB: z.string().min(1).optional(),

  // The bot's Custom Commands database (the `!` commands and the GIF-moderation
  // queue), written directly the same way as automations and utility — see
  // lib/custom-commands/store.ts. Optional; defaults to
  // ../data/custom_commands/custom_commands.db.
  VIBEY_CUSTOM_COMMANDS_DB: z.string().min(1).optional(),

  // The bot's ticketing config bridge (topics, panels, responses), read and
  // written the same way as the other bridges — see lib/ticketing/store.ts.
  // Optional; defaults to ../data/ticketing_config/ticketing_config.db.
  VIBEY_TICKETING_CONFIG_DB: z.string().min(1).optional(),

  // The bot's QOTD question-pool bridge (the question list and each guild's
  // queued "tomorrow" question), read and written the same way as the other
  // bridges — see lib/qotd/store.ts. QOTD's flat settings ride the module
  // config bridge above instead. Optional; defaults to
  // ../data/qotd_config/qotd_config.db.
  VIBEY_QOTD_CONFIG_DB: z.string().min(1).optional(),

  // The bot's NHIE config database
  VIBEY_NHIE_CONFIG_DB: z.string().min(1).optional(),

  // The bot's Game Poll database (the game library, the weekly vote ledger and
  // the draft the dashboard hands the cog to post), written directly the same
  // way as the other bridges — see lib/game-poll/store.ts. Optional; defaults
  // to ../data/game_poll_config/game_poll.db, where it sits beside the bot.
  VIBEY_GAME_POLL_DB: z.string().min(1).optional(),

  // The bot's Welcome & Onboarding bridge (the intro question pool, the
  // blacklist, per-member points, the ranked lists and game→LFG mappings),
  // read and written the same way as the other bridges — see
  // lib/onboarding/store.ts. Onboarding's flat settings ride the module config
  // bridge above. Optional; defaults to ../data/onboarding_config/onboarding_config.db.
  VIBEY_ONBOARDING_CONFIG_DB: z.string().min(1).optional(),

  // The bot's activity-tracking database (message/voice/emoji logs) and its
  // economy database (levels). Both are read strictly read-only for the
  // overview and the Activity Tracker page — see lib/bot/tracker-stats.ts.
  // Optional; default to ../tracking_data.db and ../economy.db, where they sit
  // beside the bot on the Pi. Point these at copies on a dev machine.
  VIBEY_TRACKING_DB: z.string().min(1).optional(),
  VIBEY_ECONOMY_DB: z.string().min(1).optional(),

  // The bot's Custom Matches database (games, players, MMR history, matches,
  // rivalries, live queues), read strictly read-only for the module's pages —
  // see lib/custommatch/read.ts, the same arrangement as the tracking/economy
  // databases. Optional; defaults to ../data/custommatch.db, where it sits
  // beside the bot on the Pi.
  VIBEY_CUSTOMMATCH_DB: z.string().min(1).optional(),

  // The bot's Custom Matches command bridge — the write half only. The dashboard
  // queues imperative changes here (edit a game, adjust a player's MMR, clear a
  // penalty) and the cog drains them; reads never go through it. See
  // lib/custommatch/store.ts. Optional; defaults to
  // ../data/custommatch_config/custommatch_config.db.
  VIBEY_CUSTOMMATCH_CONFIG_DB: z.string().min(1).optional(),

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
    VIBEY_OWNER_ID: process.env.VIBEY_OWNER_ID,
    VIBEY_CONTROL_URL: process.env.VIBEY_CONTROL_URL,
    VIBEY_CONTROL_TOKEN: process.env.VIBEY_CONTROL_TOKEN,
    VIBEY_AUTOMATIONS_DB: process.env.VIBEY_AUTOMATIONS_DB,
    VIBEY_UTILITY_DB: process.env.VIBEY_UTILITY_DB,
    VIBEY_CUSTOM_COMMANDS_DB: process.env.VIBEY_CUSTOM_COMMANDS_DB,
    VIBEY_ECONOMY_CONFIG_DB: process.env.VIBEY_ECONOMY_CONFIG_DB,
    VIBEY_MODULE_CONFIG_DB: process.env.VIBEY_MODULE_CONFIG_DB,
    VIBEY_TICKETING_CONFIG_DB: process.env.VIBEY_TICKETING_CONFIG_DB,
    VIBEY_QOTD_CONFIG_DB: process.env.VIBEY_QOTD_CONFIG_DB,
    VIBEY_NHIE_CONFIG_DB: process.env.VIBEY_NHIE_CONFIG_DB,
    VIBEY_GAME_POLL_DB: process.env.VIBEY_GAME_POLL_DB,
    VIBEY_ONBOARDING_CONFIG_DB: process.env.VIBEY_ONBOARDING_CONFIG_DB,
    VIBEY_TRACKING_DB: process.env.VIBEY_TRACKING_DB,
    VIBEY_ECONOMY_DB: process.env.VIBEY_ECONOMY_DB,
    VIBEY_CUSTOMMATCH_DB: process.env.VIBEY_CUSTOMMATCH_DB,
    VIBEY_CUSTOMMATCH_CONFIG_DB: process.env.VIBEY_CUSTOMMATCH_CONFIG_DB,
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
