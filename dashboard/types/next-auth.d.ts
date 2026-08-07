import type { DefaultSession } from "next-auth";
import type { DenyReason } from "@/lib/auth/authorize";

type GrantedBy = "owner" | "admin-role" | "administrator";

declare module "next-auth" {
  interface Session {
    user: {
      id: string;
      /** Which rule let them in — surfaced in the UI and the audit log. */
      grantedBy: GrantedBy | null;
      /** Set when a re-check failed; the session exists only to explain itself. */
      denied: DenyReason | null;
      /** Epoch ms of the last successful authorisation check. */
      checkedAt: number;
    } & DefaultSession["user"];
  }
}

// Augment the module the interface is actually declared in. `next-auth/jwt` is
// a bare `export * from "@auth/core/jwt"`, so declaring JWT there creates a
// second, unrelated interface instead of merging with the real one — and every
// custom claim silently types as `{}`.
declare module "@auth/core/jwt" {
  interface JWT {
    uid?: string;
    grantedBy?: GrantedBy | null;
    denied?: DenyReason | null;
    checkedAt?: number;
  }
}

export {};
