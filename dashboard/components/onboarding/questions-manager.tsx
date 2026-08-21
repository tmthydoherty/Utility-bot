"use client";

import * as React from "react";
import { ArrowDown, ArrowUp, Check, Pencil, Plus, Trash2, X } from "lucide-react";

import { cn } from "@/lib/utils";
import type { OnbQuestion } from "@/lib/onboarding/store";
import {
  addQuestion,
  deleteQuestion,
  editQuestion,
  reorderQuestions,
} from "@/app/actions/onboarding";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { useRun } from "./use-run";

export function QuestionsManager({
  guildId,
  questions,
  botOnline,
}: {
  guildId: string;
  questions: OnbQuestion[];
  botOnline: boolean;
}) {
  const { run, busy } = useRun();
  const signature = questions.map((q) => `${q.id}:${q.order}:${q.style}:${q.optional}:${q.text}`).join("|");
  const [items, setItems] = React.useState(questions);
  React.useEffect(() => setItems(questions), [signature]); // eslint-disable-line react-hooks/exhaustive-deps

  const [draft, setDraft] = React.useState("");
  const [style, setStyle] = React.useState<"short" | "long">("short");
  const [optional, setOptional] = React.useState(false);

  const move = (index: number, delta: number) => {
    const next = [...items];
    const target = index + delta;
    if (target < 0 || target >= next.length) return;
    [next[index], next[target]] = [next[target]!, next[index]!];
    setItems(next);
    run(() => reorderQuestions(guildId, next.map((q) => q.id)), { success: "Reordering questions" });
  };

  return (
    <div className="space-y-5">
      <Card className="p-5">
        <h3 className="text-sm font-semibold">Add a question</h3>
        <Input
          value={draft}
          maxLength={200}
          onChange={(e) => setDraft(e.target.value)}
          placeholder="What game got you into the community?"
          aria-label="New question"
          className="mt-3"
        />
        <div className="mt-3 flex flex-wrap items-center gap-4">
          <div className="flex items-center gap-1 rounded-md border border-[var(--border)] p-1">
            {(["short", "long"] as const).map((s) => (
              <button
                key={s}
                type="button"
                onClick={() => setStyle(s)}
                className={cn(
                  "rounded px-3 py-1 text-sm capitalize transition-colors",
                  style === s ? "bg-[var(--surface-hover)] font-medium" : "text-fg-muted",
                )}
              >
                {s === "short" ? "Short answer" : "Paragraph"}
              </button>
            ))}
          </div>
          <label className="flex items-center gap-2 text-sm text-fg-muted">
            <Switch checked={optional} onCheckedChange={setOptional} />
            Optional
          </label>
          <Button
            variant="primary"
            size="sm"
            loading={busy}
            disabled={!draft.trim()}
            className="ml-auto"
            onClick={async () => {
              const ok = await run(() => addQuestion(guildId, draft, style, optional), {
                success: "Adding the question",
              });
              if (ok) {
                setDraft("");
                setOptional(false);
                setStyle("short");
              }
            }}
          >
            <Plus aria-hidden />
            Add
          </Button>
        </div>
      </Card>

      <Card className="p-5">
        <h3 className="text-sm font-semibold">
          Question list <span className="font-normal text-fg-muted">({items.length})</span>
        </h3>
        {items.length === 0 ? (
          <EmptyState
            icon="MessageCircleQuestion"
            title="No questions yet"
            description="Add a few above — the Introduce Yourself form needs at least one."
          />
        ) : (
          <ul className="mt-4 divide-y divide-[var(--border)] rounded-lg border border-[var(--border)]">
            {items.map((q, index) => (
              <QuestionRow
                key={q.id}
                question={q}
                index={index}
                total={items.length}
                busy={busy}
                onMove={move}
                onSave={(text, style, optional) =>
                  run(() => editQuestion(guildId, q.id, text, style, optional), {
                    success: "Saving the edit",
                    optimistic: () =>
                      setItems((prev) => prev.map((x) => (x.id === q.id ? { ...x, text, style, optional } : x))),
                  })
                }
                onDelete={(shift) =>
                  run(() => deleteQuestion(guildId, q.id, shift), {
                    success: "Deleting the question",
                    optimistic: () => setItems((prev) => prev.filter((x) => x.id !== q.id)),
                  })
                }
              />
            ))}
          </ul>
        )}
      </Card>

      {!botOnline && <p className="text-xs text-fg-subtle">Changes are queued and apply when Vibey starts.</p>}
    </div>
  );
}

function QuestionRow({
  question,
  index,
  total,
  busy,
  onMove,
  onSave,
  onDelete,
}: {
  question: OnbQuestion;
  index: number;
  total: number;
  busy: boolean;
  onMove: (index: number, delta: number) => void;
  onSave: (text: string, style: "short" | "long", optional: boolean) => void;
  onDelete: (shift: boolean) => void;
}) {
  const [editing, setEditing] = React.useState(false);
  const [text, setText] = React.useState(question.text);
  const [style, setStyle] = React.useState(question.style);
  const [optional, setOptional] = React.useState(question.optional);

  React.useEffect(() => {
    setText(question.text);
    setStyle(question.style);
    setOptional(question.optional);
  }, [question.text, question.style, question.optional]);

  if (editing) {
    return (
      <li className="space-y-3 p-3">
        <Input value={text} maxLength={200} onChange={(e) => setText(e.target.value)} autoFocus aria-label="Edit question" />
        <div className="flex flex-wrap items-center gap-4">
          <div className="flex items-center gap-1 rounded-md border border-[var(--border)] p-1">
            {(["short", "long"] as const).map((s) => (
              <button
                key={s}
                type="button"
                onClick={() => setStyle(s)}
                className={cn(
                  "rounded px-3 py-1 text-sm transition-colors",
                  style === s ? "bg-[var(--surface-hover)] font-medium" : "text-fg-muted",
                )}
              >
                {s === "short" ? "Short answer" : "Paragraph"}
              </button>
            ))}
          </div>
          <label className="flex items-center gap-2 text-sm text-fg-muted">
            <Switch checked={optional} onCheckedChange={setOptional} />
            Optional
          </label>
          <div className="ml-auto flex gap-1">
            <Button
              variant="ghost"
              size="icon-sm"
              aria-label="Save"
              disabled={!text.trim()}
              onClick={() => {
                onSave(text.trim(), style, optional);
                setEditing(false);
              }}
            >
              <Check aria-hidden />
            </Button>
            <Button variant="ghost" size="icon-sm" aria-label="Cancel" onClick={() => setEditing(false)}>
              <X aria-hidden />
            </Button>
          </div>
        </div>
      </li>
    );
  }

  return (
    <li className="flex items-start gap-3 p-3">
      <div className="flex flex-col pt-0.5">
        <button
          type="button"
          aria-label="Move up"
          disabled={busy || index === 0}
          onClick={() => onMove(index, -1)}
          className="text-fg-subtle hover:text-fg disabled:opacity-30"
        >
          <ArrowUp className="size-3.5" />
        </button>
        <button
          type="button"
          aria-label="Move down"
          disabled={busy || index === total - 1}
          onClick={() => onMove(index, 1)}
          className="text-fg-subtle hover:text-fg disabled:opacity-30"
        >
          <ArrowDown className="size-3.5" />
        </button>
      </div>
      {/* Text and its badges share one flex-wrap row. The text keeps a minimum
          width so on a narrow phone the badges wrap onto the next line instead
          of squeezing the text down to one-letter-per-line. */}
      <div className="flex min-w-0 flex-1 flex-wrap items-center gap-x-2 gap-y-1.5">
        <span className="min-w-[10rem] flex-1 break-words text-sm">{question.text}</span>
        {question.optional && <Badge variant="neutral">optional</Badge>}
        <Badge variant="neutral">{question.style === "long" ? "paragraph" : "short"}</Badge>
      </div>
      <div className="flex shrink-0 gap-1">
        <Button variant="ghost" size="icon-sm" aria-label="Edit" disabled={busy} onClick={() => setEditing(true)}>
          <Pencil aria-hidden />
        </Button>
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label="Delete"
          disabled={busy}
          onClick={() => {
            if (!confirm("Delete this question?")) return;
            const shift = confirm("Shift the questions below up to close the gap? Cancel to leave their order numbers as-is.");
            onDelete(shift);
          }}
        >
          <Trash2 aria-hidden />
        </Button>
      </div>
    </li>
  );
}
