"use client";

import * as React from "react";
import { Plus, Trash2 } from "lucide-react";

import type { OnbBlacklistEntry } from "@/lib/onboarding/store";
import { blacklistAdd, blacklistRemove } from "@/app/actions/onboarding";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Input } from "@/components/ui/input";
import { useRun } from "./use-run";

const SNOWFLAKE = /^\d{17,20}$/;

export function BlacklistManager({
  guildId,
  blacklist,
  botOnline,
}: {
  guildId: string;
  blacklist: OnbBlacklistEntry[];
  botOnline: boolean;
}) {
  const { run, busy } = useRun();
  const [userId, setUserId] = React.useState("");
  const valid = SNOWFLAKE.test(userId.trim());

  return (
    <div className="space-y-5">
      <Card className="p-5">
        <h3 className="text-sm font-semibold">Block a member</h3>
        <p className="mt-1 text-sm text-fg-muted">Enter a member ID to block them from the Introduce Yourself button.</p>
        <div className="mt-3 flex flex-wrap gap-2">
          <Input
            value={userId}
            onChange={(e) => setUserId(e.target.value)}
            placeholder="Member ID"
            aria-label="Member ID to block"
            className="min-w-[12rem] flex-1"
          />
          <Button
            variant="primary"
            size="sm"
            loading={busy}
            disabled={!valid}
            onClick={async () => {
              const ok = await run(() => blacklistAdd(guildId, userId.trim()), { success: "Blocking the member" });
              if (ok) setUserId("");
            }}
          >
            <Plus aria-hidden />
            Block
          </Button>
        </div>
      </Card>

      <Card className="p-5">
        <h3 className="text-sm font-semibold">
          Blocked members <span className="font-normal text-fg-muted">({blacklist.length})</span>
        </h3>
        {blacklist.length === 0 ? (
          <EmptyState icon="ShieldCheck" title="Nobody's blocked" description="Blocked members can't post an introduction." />
        ) : (
          <ul className="mt-4 divide-y divide-[var(--border)] rounded-lg border border-[var(--border)]">
            {blacklist.map((b) => (
              <li key={b.userId} className="flex items-center gap-3 p-3 text-sm">
                <span className="min-w-0 flex-1 truncate">
                  {b.name} <span className="text-fg-subtle">· {b.userId}</span>
                </span>
                <Button
                  variant="ghost"
                  size="icon-sm"
                  aria-label={`Unblock ${b.name}`}
                  disabled={busy}
                  onClick={() => run(() => blacklistRemove(guildId, b.userId), { success: "Unblocking the member" })}
                >
                  <Trash2 aria-hidden />
                </Button>
              </li>
            ))}
          </ul>
        )}
      </Card>

      {!botOnline && <p className="text-xs text-fg-subtle">Changes are queued and apply when Vibey starts.</p>}
    </div>
  );
}
