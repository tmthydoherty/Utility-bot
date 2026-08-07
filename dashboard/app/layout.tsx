import type { Metadata, Viewport } from "next";
import { headers } from "next/headers";
import { GeistMono } from "geist/font/mono";
import { GeistSans } from "geist/font/sans";

import { themeInitScript } from "@/components/providers/theme-provider";
import { Providers } from "./providers";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "Vibey Dashboard",
    template: "%s · Vibey",
  },
  description: "Configure and monitor the Vibey Discord bot.",
  applicationName: "Vibey",
  manifest: "/manifest.webmanifest",
  appleWebApp: {
    capable: true,
    title: "Vibey",
    statusBarStyle: "black-translucent",
  },
  // Nothing here should ever be indexed: it is a private admin surface, and
  // the login page appearing in search results is free reconnaissance.
  robots: { index: false, follow: false, nocache: true },
  icons: {
    icon: [{ url: "/icons/icon.svg", type: "image/svg+xml" }],
    apple: [{ url: "/icons/apple-touch-icon.png", sizes: "180x180" }],
  },
};

export const viewport: Viewport = {
  themeColor: "#0a0a0f",
  colorScheme: "dark light",
  width: "device-width",
  initialScale: 1,
  // Zoom stays available. Locking it is an accessibility failure, and the
  // layout is built so nothing needs it anyway.
  maximumScale: 5,
  viewportFit: "cover",
};

export default async function RootLayout({ children }: { children: React.ReactNode }) {
  // Set by middleware. The theme script below is the only inline script in the
  // app and it carries this nonce so the CSP never needs 'unsafe-inline'.
  const nonce = (await headers()).get("x-nonce") ?? undefined;

  return (
    <html lang="en" suppressHydrationWarning className={`${GeistSans.variable} ${GeistMono.variable}`}>
      <head>
        {/* React deliberately blanks the nonce attribute client-side so a
            script can't read it back out of the DOM and reuse it. That makes
            the attribute differ from the server HTML by design, so the
            mismatch has to be suppressed on this element specifically. */}
        <script
          nonce={nonce}
          suppressHydrationWarning
          dangerouslySetInnerHTML={{ __html: themeInitScript }}
        />
      </head>
      <body className="min-h-dvh antialiased">
        {/* Skip link: the first thing a keyboard user hits, and invisible until
            focused. Without it, reaching page content means tabbing the whole
            navigation on every single page. */}
        <a
          href="#main"
          className="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-[100] focus:rounded-md focus:bg-[var(--surface-solid)] focus:px-4 focus:py-2 focus:text-sm focus:shadow-[var(--elev-3)]"
        >
          Skip to content
        </a>
        <div className="aurora" aria-hidden>
          <span />
        </div>
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
