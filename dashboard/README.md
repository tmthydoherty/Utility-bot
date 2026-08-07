# Vibey Dashboard

A web dashboard for the Vibey Discord bot. Next.js 15, Tailwind v4, Auth.js,
SQLite. Runs on the same Raspberry Pi as the bot and is served through the
cloudflared tunnel that is already running.

**This pass is foundation and UI.** Three modules have real settings pages;
the other ~32 are listed but still configured through Discord. Nothing here
writes to the bot's configs, its databases, or Discord — saves land in this
project's own SQLite file. See `lib/bot/adapter.ts`, which is the seam that
gets swapped when the cogs are wired up.

---

## Running it

```bash
cd /home/tmthy/Vibey/dashboard
npm install
npm run dev          # http://localhost:3100
```

Production:

```bash
npm run build && npm start
```

| Script | |
|---|---|
| `npm run dev` | Dev server on 3100 |
| `npm run build` | Production build |
| `npm start` | Serve the build on 3100 |
| `npm run typecheck` | `tsc --noEmit` |
| `npm run lint` | ESLint |

## Configuration

Copy `.env.example` to `.env.local` and `chmod 600` it. Every variable is
required and validated at boot by `lib/env.ts` — a missing one fails loudly
rather than at the first request that needs it.

`DISCORD_BOT_TOKEN` is the same token the Python bot uses. It is server-side
only and never reaches the browser.

## How access control works

1. Sign in with Discord (`identify` scope only).
2. `lib/auth/authorize.ts` looks the user up **with the bot token** and allows
   them if they own the server, hold `VIBEY_ADMIN_ROLE_ID`, or hold any role
   with the Administrator permission. This mirrors `Vibey.is_bot_admin` in
   `main.py` exactly.
3. The user's own OAuth token is discarded after sign-in. Nothing replayable
   is stored in the session cookie.
4. The check re-runs at most every 5 minutes. Losing the admin role ends the
   session at the next check — no restart needed.
5. Anything other than an explicit "yes" is a denial. A Discord outage keeps an
   already-verified session alive for up to 30 minutes and then closes it.

Security headers (CSP with a per-request nonce, HSTS, frame-ancestors none) and
the cross-origin check are applied in `middleware.ts`. Rate limiting and the
audit log are in `lib/db/`.

## Adding a module

Add a `ModuleSchema` to `lib/schema/modules.ts`. Types come from
`lib/schema/types.ts`, which mirrors `cogs/utility/schema.py` — the same `Field`
model the bot's own Discord panels are built from.

```ts
{
  id: "qotd",
  name: "Question of the Day",
  description: "A daily prompt posted on a schedule.",
  icon: "MessageCircleQuestion",   // must exist in components/ui/icon.tsx
  category: "engagement",
  cog: "cogs/qotd.py",
  configurable: true,
  sections: [{
    id: "channels",
    title: "Channels",
    fields: [
      { key: "channel_id", label: "Post in", type: FieldType.CHANNEL, required: true },
      { key: "hour", label: "Hour (UTC)", type: FieldType.NUMBER, min: 0, max: 23 },
    ],
  }],
}
```

No UI work is needed — `FieldRenderer` already maps every `FieldType` to a
control, and validation is derived from the schema in `lib/schema/validate.ts`.

## Deploying

```bash
sudo cp deploy/vibey-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now vibey-dashboard
sudo systemctl status vibey-dashboard
```

Then merge `deploy/cloudflared-ingress.yml` into `/etc/cloudflared/config.yml`
(above the `http_status:404` catch-all) and
`sudo systemctl restart cloudflared`.

### One-time manual steps

1. **Cloudflare** — add `vibe-y.us` as a site and point the registrar's
   nameservers at the two Cloudflare provides.
2. **DNS** — CNAME `@` and `www` →
   `d983157c-4adc-4417-8ddf-e849632bd3da.cfargotunnel.com`, proxied.
3. **Discord Developer Portal** → OAuth2 → add redirect URLs
   `https://vibe-y.us/api/auth/callback/discord` and
   `http://localhost:3100/api/auth/callback/discord`, then put the Client ID and
   Client Secret in `.env.local`.
4. Set `AUTH_URL=https://vibe-y.us` in `.env.local` once DNS is live.

## Layout

```
app/               routes, server actions
components/
  ui/              design-system primitives
  shell/           sidebar, mobile tab bar, top bar, command palette
  settings/        schema-driven form + field renderer
  charts/          hand-rolled SVG chart and sparkline
  providers/       theme + guild context
lib/
  auth/            the authorisation rule
  db/              SQLite: audit log, rate limiter, drafts
  discord/         bot-token REST client
  schema/          Field model + the module catalogue
  bot/adapter.ts   the seam to the bot
deploy/            systemd unit + tunnel ingress
```
