"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Check, Pencil, Plus, RefreshCw, Search, Trash2, X } from "lucide-react";

import { cn } from "@/lib/utils";
import type { QotdPool, QotdQuestion } from "@/lib/qotd/store";
import {
  addQuestions,
  clearSeen,
  deleteQuestion,
  editQuestion,
  rerollTomorrow,
  resetPool,
  type ActionResult,
} from "@/app/actions/qotd";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Input, Textarea } from "@/components/ui/input";
import { useToast } from "@/components/ui/toast";

/**
 * The Question of the Day pool manager.
 *
 * Everything here queues a command onto the bot's bridge rather than writing the
 * question database directly, so a change is applied by the cog within about ten
 * seconds. The list updates optimistically so an edit feels immediate, then a
 * delayed refresh reconciles against the bot's republished snapshot — the same
 * eventual-consistency the ticketing publish flow has, made to feel instant.
 */

// How long to wait before re-reading the bot's snapshot. The cog polls every
// ten seconds; a hair more than that and the reconcile almost always catches
// the applied change on the first try.
const RECONCILE_MS = 11_000;

export function QotdQuestions({
  guildId,
  pool,
  botOnline,
  autoPosting,
}: {
  guildId: string;
  pool: QotdPool;
  botOnline: boolean;
  autoPosting: boolean;
}) {
  const router = useRouter();
  const toast = useToast();

  // Local copy so edits/deletes/adds can show at once; re-synced from the server
  // whenever the real pool changes (after the bot applies a queued command).
  const signature = React.useMemo(
    () => pool.questions.map((q) => `${q.id}:${q.seen ? 1 : 0}:${q.text}`).join("|"),
    [pool.questions],
  );
  const [items, setItems] = React.useState<QotdQuestion[]>(pool.questions);
  React.useEffect(() => setItems(pool.questions), [signature]); // eslint-disable-line react-hooks/exhaustive-deps

  const [query, setQuery] = React.useState("");
  const [draft, setDraft] = React.useState("");
  const [busy, setBusy] = React.useState(false);

  const reconcile = React.useCallback(() => {
    window.setTimeout(() => router.refresh(), RECONCILE_MS);
  }, [router]);

  // Run an action, surface its result, and schedule a reconcile refresh. The
  // optional optimistic update is applied only when the queue accepted the
  // command, so a failure never leaves a phantom row behind.
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

  const unseen = items.filter((q) => !q.seen).length;

  return (
    <div className="space-y-5">
      {!botOnline && (
        <Badge variant="warning">
          Vibey isn&apos;t running — the pool below is a snapshot, and changes apply when it starts.
        </Badge>
      )}

      <TomorrowCard
        pool={pool}
        botOnline={botOnline}
        autoPosting={autoPosting}
        busy={busy}
        onReroll={() =>
          run(() => rerollTomorrow(guildId), { success: "Picking another question" })
        }
      />

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
          placeholder={"What's a hill you'll die on?\nBest meal you've had this year?"}
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
                      id: -(Date.now() + i), // provisional; replaced on reconcile
                      text,
                      addedById: null,
                      timesUsed: 0,
                      seen: false,
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
              <span className="font-medium text-fg">{items.length}</span> total ·{" "}
              <span className="font-medium text-fg">{unseen}</span> not yet asked
            </p>
          </div>
          <div className="flex gap-2">
            <Button
              variant="ghost"
              size="sm"
              disabled={busy || items.every((q) => !q.seen)}
              onClick={() => {
                if (!confirm("Make every question available to be asked again?")) return;
                run(() => resetPool(guildId), { success: "Resetting the pool" });
              }}
            >
              <RefreshCw aria-hidden />
              Reset pool
            </Button>
            <Button
              variant="ghost"
              size="sm"
              disabled={busy || items.every((q) => !q.seen)}
              onClick={() => {
                if (!confirm("Permanently delete every question that's already been asked?")) return;
                run(() => clearSeen(guildId), { success: "Clearing asked questions" });
              }}
            >
              <Trash2 aria-hidden />
              Clear asked
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
              description="Add a few above and they'll start posting on your schedule."
            />
          ) : filtered.length === 0 ? (
            <p className="py-8 text-center text-sm text-fg-muted">Nothing matches that search.</p>
          ) : (
            <ul className="max-h-[28rem] divide-y divide-[var(--border)] overflow-y-auto rounded-lg border border-[var(--border)]">
              {filtered.map((q) => (
                <QuestionRow
                  key={q.id}
                  question={q}
                  busy={busy}
                  onSave={(text) =>
                    run(() => editQuestion(guildId, q.id, text), {
                      success: "Saving the edit",
                      optimistic: () =>
                        setItems((prev) => prev.map((x) => (x.id === q.id ? { ...x, text } : x))),
                    })
                  }
                  onDelete={() =>
                    run(() => deleteQuestion(guildId, q.id), {
                      success: "Deleting the question",
                      optimistic: () => setItems((prev) => prev.filter((x) => x.id !== q.id)),
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

function TomorrowCard({
  pool,
  botOnline,
  autoPosting,
  busy,
  onReroll,
}: {
  pool: QotdPool;
  botOnline: boolean;
  autoPosting: boolean;
  busy: boolean;
  onReroll: () => void;
}) {
  return (
    <Card className="p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="text-sm font-semibold">
            {autoPosting ? "Next up" : "Would post next"}
          </h3>
          <p className="mt-1 text-sm text-fg-muted">
            {autoPosting
              ? "The question queued to post at your next scheduled time."
              : "Automatic posting is off, but this is what a manual post would send."}
          </p>
        </div>
        <Button
          variant="secondary"
          size="sm"
          loading={busy}
          disabled={!botOnline || pool.unseen === 0}
          onClick={onReroll}
        >
          <RefreshCw aria-hidden />
          Pick another
        </Button>
      </div>

      <div className="mt-4 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4">
        {pool.tomorrow ? (
          <p className="text-base font-medium">{pool.tomorrow.text}</p>
        ) : pool.unseen === 0 ? (
          <p className="text-sm text-fg-muted">
            Nothing queued — every question has been asked. Add more above, or reset the pool.
          </p>
        ) : (
          <p className="text-sm text-fg-muted">
            No question queued yet.{" "}
            {autoPosting
              ? "Vibey will pick one shortly."
              : "Turn on automatic posting, or use “Pick another”."}
          </p>
        )}
      </div>
    </Card>
  );
}

function QuestionRow({
  question,
  busy,
  onSave,
  onDelete,
}: {
  question: QotdQuestion;
  busy: boolean;
  onSave: (text: string) => void;
  onDelete: () => void;
}) {
  const [editing, setEditing] = React.useState(false);
  const [text, setText] = React.useState(question.text);
  const pending = question.id < 0;

  React.useEffect(() => setText(question.text), [question.text]);

  if (editing) {
    return (
      <li className="flex items-center gap-2 p-3">
        <Input
          value={text}
          maxLength={256}
          onChange={(e) => setText(e.target.value)}
          autoFocus
          aria-label="Edit question"
          onKeyDown={(e) => {
            if (e.key === "Enter" && text.trim()) {
              onSave(text.trim());
              setEditing(false);
            } else if (e.key === "Escape") {
              setText(question.text);
              setEditing(false);
            }
          }}
        />
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label="Save"
          disabled={!text.trim() || text.trim() === question.text}
          onClick={() => {
            onSave(text.trim());
            setEditing(false);
          }}
        >
          <Check aria-hidden />
        </Button>
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label="Cancel"
          onClick={() => {
            setText(question.text);
            setEditing(false);
          }}
        >
          <X aria-hidden />
        </Button>
      </li>
    );
  }

  return (
    <li className={cn("flex items-center gap-3 p-3", pending && "opacity-60")}>
      <span className="min-w-0 flex-1 break-words text-sm">{question.text}</span>
      {pending ? (
        <Badge variant="neutral">saving…</Badge>
      ) : (
        question.seen && <Badge variant="neutral">asked</Badge>
      )}
      <div className="flex shrink-0 gap-1">
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label="Edit question"
          disabled={busy || pending}
          onClick={() => setEditing(true)}
        >
          <Pencil aria-hidden />
        </Button>
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
