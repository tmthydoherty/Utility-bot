import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // The Pi serves this behind cloudflared; the tunnel already sets the real
  // scheme/host headers, and leaking the framework version buys an attacker
  // a free CVE lookup.
  poweredByHeader: false,
  experimental: {
    // Only the icons actually imported get bundled, rather than the whole set.
    optimizePackageImports: ["lucide-react", "motion"],
  },
  images: {
    remotePatterns: [
      { protocol: "https", hostname: "cdn.discordapp.com" },
      { protocol: "https", hostname: "media.discordapp.net" },
    ],
  },
  // better-sqlite3 is a native addon: it must stay a real require() on the
  // server rather than being traced into the bundle.
  serverExternalPackages: ["better-sqlite3"],
};

export default nextConfig;
