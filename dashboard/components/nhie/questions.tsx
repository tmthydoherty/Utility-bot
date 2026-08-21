"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Plus, RefreshCw, Search, Trash2 } from "lucide-react";

import { cn } from "@/lib/utils";
import type { NhiePool, NhieQuestion } from "@/lib/nhie/store";
import {
  addQuestions,
  deleteQuestion,
  resetPool,
  type ActionResult,
} from "@/app/actions/nhie";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Input, Textarea } from "@/components/ui/input";
import { useToast } from "@/components/ui/toast";

const RECONCILE_MS = 11_000;

export function NhieQuestions({
  guildId,
  pool,
  botOnline,
}: {
  guildId: string;
  pool: NhiePool;
  botOnline: boolean;
}) {
  const router = useRouter();
  const toast = useToast();

  const signature = React.useMemo(
    () => pool.questions.map((q) => `${q.index}:${q.text}`).join("|"),
    [pool.questions],
  );
  const [items, setItems] = React.useState<NhieQuestion[]>(pool.questions);
  React.useEffect(() => setItems(pool.questions), [signature]); // eslint-disable-line react-hooks/exhaustive-deps

  const [query, setQuery] = React.useState("");
  const [draft, setDraft] = React.useState("");
  const [busy, setBusy] = React.useState(false);

  const reconcile = React.useCallback(() => {
    window.setTimeout(() => router.refresh(), RECONCILE_MS);
  }, [router]);

  const run = React.useCallback(
    async (
      fn: () => Promise<ActionResult>,
      { success, optimistic }: { success: string; optimistic?: () => void },
    ) => {
      setBusy(true);
      try {
        const result = await fn();
        if (result.ok) {
          optimistic?.();
          toast.success(success, "Vibey applies this within about 10 seconds.");
          reconcile();
          return true;
        }
        toast.error("Couldn't save that", result.error ?? "Try again.");
        return false;
      } catch {
        toast.error("Couldn't reach the server", "Nothing was changed.");
        return false;
      } finally {
        setBusy(false);
      }
    },
    [toast, reconcile],
  );

  const filtered = React.useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return items;
    return items.filter((q) => q.text.toLowerCase().includes(needle));
  }, [items, query]);

  return (
    <div className="space-y-5">
      {!botOnline && (
        <Badge variant="warning">
          Vibey isn&apos;t running — the pool below is a snapshot, and changes apply when it starts.
        </Badge>
      )}

      {/* Add questions */}
      <Card className="p-5">
        <h3 className="text-sm font-semibold">Add questions</h3>
        <p className="mt-1 text-sm text-fg-muted">
          One per line. Duplicates are skipped automatically.
        </p>
        <Textarea
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          rows={4}
          placeholder={"Never have I ever gone skydiving\nNever have I ever broken a bone"}
          className="mt-3"
          aria-label="New questions, one per line"
        />
        <div className="mt-3 flex justify-end">
          <Button
            variant="primary"
            size="sm"
            loading={busy}
            disabled={!draft.trim()}
            onClick={async () => {
              const lines = draft
                .split("\n")
                .map((l) => l.trim())
                .filter(Boolean);
              const ok = await run(() => addQuestions(guildId, draft), {
                success: `Adding ${lines.length} question${lines.length === 1 ? "" : "s"}`,
                optimistic: () =>
                  setItems((prev) => [
                    ...prev,
                    ...lines.map((text, i) => ({
                      index: -(Date.now() + i), // provisional; replaced on reconcile
                      text,
                      type: "nhie",
                      suggesterId: null,
                    })),
                  ]),
              });
              if (ok) setDraft("");
            }}
          >
            <Plus aria-hidden />
            Add
          </Button>
        </div>
      </Card>

      {/* The pool */}
      <Card className="p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h3 className="text-sm font-semibold">Question pool</h3>
            <p className="mt-1 text-sm text-fg-muted">
              <span className="font-medium text-fg">{items.length}</span> total questions
            </p>
          </div>
          <div className="flex gap-2">
            <Button
              variant="ghost"
              size="sm"
              disabled={busy || items.length === 0}
              onClick={() => {
                if (!confirm("Delete all questions?")) return;
                run(() => resetPool(guildId), { success: "Resetting the pool" });
              }}
            >
              <RefreshCw aria-hidden />
              Reset pool
            </Button>
          </div>
        </div>

        <div className="relative mt-4">
          <Search
            className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-fg-subtle"
            aria-hidden
          />
          <Input
            type="search"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search questions…"
            aria-label="Search questions"
            className="pl-9"
          />
        </div>

        <div className="mt-4">
          {items.length === 0 ? (
            <EmptyState
              icon="MessageCircleQuestion"
              title="No questions yet"
              description="Add a few above and they'll start appearing in the module."
            />
          ) : filtered.length === 0 ? (
            <p className="py-8 text-center text-sm text-fg-muted">Nothing matches that search.</p>
          ) : (
            <ul className="max-h-[28rem] divide-y divide-[var(--border)] overflow-y-auto rounded-lg border border-[var(--border)]">
              {filtered.map((q) => (
                <QuestionRow
                  key={q.index}
                  question={q}
                  busy={busy}
                  onDelete={() =>
                    run(() => deleteQuestion(guildId, q.index), {
                      success: "Deleting the question",
                      optimistic: () => setItems((prev) => prev.filter((x) => x.index !== q.index)),
                    })
                  }
                />
              ))}
            </ul>
          )}
        </div>
      </Card>
    </div>
  );
}

function QuestionRow({
  question,
  busy,
  onDelete,
}: {
  question: NhieQuestion;
  busy: boolean;
  onDelete: () => void;
}) {
  const pending = question.index < 0;

  return (
    <li className={cn("flex items-center gap-3 p-3", pending && "opacity-60")}>
      <span className="min-w-0 flex-1 break-words text-sm">{question.text}</span>
      {pending && <Badge variant="neutral">saving…</Badge>}
      <div className="flex shrink-0 gap-1">
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label="Delete question"
          disabled={busy || pending}
          onClick={() => {
            if (confirm("Delete this question?")) onDelete();
          }}
        >
          <Trash2 aria-hidden />
        </Button>
      </div>
    </li>
  );
}
