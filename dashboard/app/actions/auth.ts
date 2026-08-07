"use server";

import { headers } from "next/headers";
import { AuthError } from "next-auth";
import { redirect } from "next/navigation";

import { signIn, signOut } from "@/auth";
import { checkRateLimit, clientIp } from "@/lib/db/rate-limit";

/**
 * Sign-in, rate-limited by IP.
 *
 * The limit is here rather than in middleware because the edge runtime cannot
 * reach SQLite — and this is the right place anyway: it bounds attempts at the
 * point where a session could actually be created.
 */
export async function startDiscordSignIn(next?: string) {
  const headerList = await headers();
  const ip = clientIp(headerList);

  const limit = checkRateLimit("signin", ip);
  if (!limit.allowed) {
    const seconds = Math.ceil((limit.resetAt - Date.now()) / 1000);
    redirect(`/auth/error?error=RateLimited&retry=${seconds}`);
  }

  // Only same-origin paths are honoured, so `?next=` cannot be used to bounce
  // someone to another site after a successful login.
  const target = next && next.startsWith("/") && !next.startsWith("//") ? next : "/dashboard";

  try {
    await signIn("discord", { redirectTo: target });
  } catch (error) {
    // next/navigation signals redirects by throwing; rethrow so Next can
    // handle it rather than swallowing it as a failure.
    if (error instanceof AuthError) {
      redirect(`/auth/error?error=${encodeURIComponent(error.type)}`);
    }
    throw error;
  }
}

export async function endSession() {
  await signOut({ redirectTo: "/" });
}
