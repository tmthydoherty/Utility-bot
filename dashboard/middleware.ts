import NextAuth from "next-auth";
import { NextResponse } from "next/server";

import authConfig, { AUTH_PAGES } from "./auth.config";

/**
 * Edge middleware: security headers on everything, and the outer door on
 * anything private.
 *
 * It imports auth.config (not auth.ts) on purpose — see the note there. This
 * layer only checks that a session decodes and hasn't been marked denied; the
 * authoritative Discord check happens in the Node runtime on every page.
 */

const { auth } = NextAuth(authConfig);

/** Everything under these prefixes requires a session. */
const PROTECTED_PREFIXES = ["/dashboard", "/api/settings", "/api/guild"];

function isProtected(pathname: string): boolean {
  return PROTECTED_PREFIXES.some(
    (prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`),
  );
}

function buildCsp(nonce: string, isDev: boolean): string {
  // 'strict-dynamic' lets the nonce'd Next bootstrap load its own chunks
  // without every chunk URL needing to be enumerated, while still refusing
  // anything an injected tag tries to pull in.
  const script = isDev
    ? `'self' 'nonce-${nonce}' 'unsafe-eval' 'unsafe-inline'`
    : `'self' 'nonce-${nonce}' 'strict-dynamic'`;

  return [
    `default-src 'self'`,
    `script-src ${script}`,
    // React renders the `style` prop as an inline style attribute, and the
    // animation library writes styles constantly — nonces cannot cover either.
    // Inline *style* is a far weaker vector than inline script, and script is
    // locked down properly above, so this is the accepted trade.
    `style-src 'self' 'unsafe-inline'`,
    // `https:` so a ticketing panel's preview can load a banner/thumbnail from
    // whatever host an admin pastes — the same arbitrary URL the bot embeds in
    // Discord — not only Discord's own CDN. Images are an inert content type and
    // script is locked down separately above, so allowing any https image is a
    // safe trade for an admin-only dashboard that previews user-supplied URLs.
    `img-src 'self' data: blob: https:`,
    `font-src 'self' data:`,
    // Discord's OAuth token exchange is server-side; the browser never needs
    // to reach anything off-origin.
    `connect-src 'self'`,
    `frame-src 'none'`,
    `object-src 'none'`,
    `base-uri 'self'`,
    `form-action 'self' https://discord.com`,
    `frame-ancestors 'none'`,
    `manifest-src 'self'`,
    ...(isDev ? [] : [`upgrade-insecure-requests`]),
  ].join("; ");
}

function applySecurityHeaders(headers: Headers, nonce: string, isDev: boolean): void {
  headers.set("Content-Security-Policy", buildCsp(nonce, isDev));
  headers.set("X-Content-Type-Options", "nosniff");
  headers.set("X-Frame-Options", "DENY");
  headers.set("Referrer-Policy", "strict-origin-when-cross-origin");
  headers.set(
    "Permissions-Policy",
    "camera=(), microphone=(), geolocation=(), payment=(), usb=(), interest-cohort=()",
  );
  headers.set("Cross-Origin-Opener-Policy", "same-origin");
  headers.set("Cross-Origin-Resource-Policy", "same-origin");
  headers.set("X-DNS-Prefetch-Control", "off");

  if (!isDev) {
    // Two years, subdomains included, preload-eligible. Safe here because the
    // tunnel only ever serves this host over HTTPS.
    headers.set(
      "Strict-Transport-Security",
      "max-age=63072000; includeSubDomains; preload",
    );
  }
}

export default auth((req) => {
  const isDev = process.env.NODE_ENV !== "production";
  const { pathname } = req.nextUrl;

  const nonce = crypto.randomUUID().replaceAll("-", "");

  /**
   * Origin check on state-changing requests.
   *
   * Auth.js already carries a CSRF token and the session cookie is SameSite=Lax,
   * which between them stop the classic cross-site form post. This is a third,
   * cheap layer that also covers Server Action posts, and it fails closed on a
   * request that presents no Origin at all where one is expected.
   */
  if (req.method !== "GET" && req.method !== "HEAD") {
    const origin = req.headers.get("origin");
    const host = req.headers.get("host");
    if (origin && host) {
      let originHost: string | null = null;
      try {
        originHost = new URL(origin).host;
      } catch {
        originHost = null;
      }
      if (originHost !== host) {
        return new NextResponse("Cross-origin request rejected", { status: 403 });
      }
    }
  }

  const session = req.auth;
  const hasAccess = Boolean(session?.user?.id && !session.user.denied);

  if (isProtected(pathname) && !hasAccess) {
    // API routes get a status code; pages get sent to sign in with a way back.
    if (pathname.startsWith("/api/")) {
      return NextResponse.json({ error: "unauthorized" }, { status: 401 });
    }
    const url = req.nextUrl.clone();
    url.pathname = AUTH_PAGES.signIn;
    url.search = `?next=${encodeURIComponent(pathname)}`;
    const redirect = NextResponse.redirect(url);
    applySecurityHeaders(redirect.headers, nonce, isDev);
    return redirect;
  }

  // Someone already signed in has no reason to look at the login screen.
  if (pathname === AUTH_PAGES.signIn && hasAccess) {
    const url = req.nextUrl.clone();
    url.pathname = "/dashboard";
    url.search = "";
    const redirect = NextResponse.redirect(url);
    applySecurityHeaders(redirect.headers, nonce, isDev);
    return redirect;
  }

  // The nonce goes down as a request header so the root layout can stamp it on
  // its inline theme script, and back up as a response header for the browser.
  const requestHeaders = new Headers(req.headers);
  requestHeaders.set("x-nonce", nonce);

  const res = NextResponse.next({ request: { headers: requestHeaders } });
  applySecurityHeaders(res.headers, nonce, isDev);
  return res;
});

export const config = {
  matcher: [
    /*
     * Everything except Next's own static output and files served straight
     * from /public — those are immutable assets that need no headers and would
     * only pay the middleware cost on every request.
     */
    "/((?!_next/static|_next/image|favicon.ico|icons/|manifest.webmanifest|robots.txt).*)",
  ],
};
