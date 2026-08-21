"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { ChevronDown, Send, Trash2 } from "lucide-react";

import { cn, formatRelative } from "@/lib/utils";
import type { SurveyResponse, Topic } from "@/lib/ticketing/types";
import { deleteResponse, deleteResponses, sendSurvey } from "@/app/actions/ticketing";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { EmptyState } from "@/components/ui/empty-state";
import { useToast } from "@/components/ui/toast";
import { RolesField, IdListField, SelectField, Section } from "./controls";

interface SurveyGroup {
  name: string;
  label: string;
  responses: SurveyResponse[];
}

export function ResponsesView({
  guildId,
  groups,
  askableTopics,
}: {
  guildId: string;
  groups: SurveyGroup[];
  /** Topics with questions — the ones a survey can be sent for. */
  askableTopics: Pick<Topic, "name" | "label">[];
}) {
  return (
    <div className="space-y-6">
      <SendSurvey guildId={guildId} topics={askableTopics} />

      {groups.length === 0 ? (
        <EmptyState
          icon="Inbox"
          title="No responses yet"
          description="Answers to applications and surveys show up here as people submit them."
        />
      ) : (
        <div className="space-y-3">
          {groups.map((g) => (
            <SurveyCard key={g.name} guildId={guildId} group={g} />
          ))}
        </div>
      )}
    </div>
  );
}

function SurveyCard({ guildId, group }: { guildId: string; group: SurveyGroup }) {
  const router = useRouter();
  const toast = useToast();
  const [open, setOpen] = React.useState(false);
  const [busy, setBusy] = React.useState(false);

  const removeAll = async () => {
    if (!confirm(`Delete all ${group.responses.length} responses for "${group.label}"? This can't be undone.`)) return;
    setBusy(true);
    const result = await deleteResponses(guildId, group.name);
    setBusy(false);
    if (result.ok) {
      toast.success("Responses deleted", `Cleared ${group.label}.`);
      router.refresh();
    } else {
      toast.error("Couldn't delete", result.error ?? "Try again.");
    }
  };

  const exportJson = () => {
    const blob = new Blob([JSON.stringify(group.responses, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${group.name}-responses.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="glass rounded-xl">
      <div className="flex flex-wrap items-center gap-3 p-4">
        <button
          type="button"
          onClick={() => setOpen((o) => !o)}
          className="flex min-w-0 flex-1 items-center gap-2 text-left"
          aria-expanded={open}
        >
          <ChevronDown
            className={cn("size-4 shrink-0 text-fg-subtle transition-transform", open && "rotate-180")}
            aria-hidden
          />
          <span className="truncate font-medium">{group.label}</span>
          <code className="hidden text-xs text-fg-subtle sm:inline">{group.name}</code>
          <Badge variant="neutral">{group.responses.length}</Badge>
        </button>
        <div className="flex gap-2">
          <Button variant="ghost" size="sm" onClick={exportJson}>
            Export
          </Button>
          <Button variant="ghost" size="sm" onClick={removeAll} loading={busy}>
            <Trash2 aria-hidden />
            Delete all
          </Button>
        </div>
      </div>

      {open && (
        <div className="space-y-3 border-t border-[var(--border)] p-4">
          {group.responses.length === 0 && (
            <p className="text-sm text-fg-muted">No responses.</p>
          )}
          {group.responses.map((r, i) => (
            <ResponseCard key={`${r.userId}-${r.timestamp}-${i}`} guildId={guildId} survey={group.name} response={r} />
          ))}
        </div>
      )}
    </div>
  );
}

function ResponseCard({
  guildId,
  survey,
  response,
}: {
  guildId: string;
  survey: string;
  response: SurveyResponse;
}) {
  const router = useRouter();
  const toast = useToast();
  const [busy, setBusy] = React.useState(false);

  const remove = async () => {
    if (!confirm(`Delete this response from ${response.userName || response.userId}?`)) return;
    setBusy(true);
    const result = await deleteResponse(guildId, survey, response.userId, response.timestamp);
    setBusy(false);
    if (result.ok) {
      toast.success("Response deleted", "");
      router.refresh();
    } else {
      toast.error("Couldn't delete", result.error ?? "Try again.");
    }
  };

  return (
    <div className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3">
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-medium">{response.userName || response.userId}</span>
        <code className="text-xs text-fg-subtle">{response.userId}</code>
        {response.timestamp && (
          <span className="text-xs text-fg-subtle">{formatRelative(Date.parse(response.timestamp))}</span>
        )}
        <Button variant="ghost" size="icon-sm" onClick={remove} loading={busy} aria-label="Delete response" className="ml-auto">
          <Trash2 aria-hidden />
        </Button>
      </div>
      <dl className="mt-2 space-y-2">
        {Object.entries(response.answers).map(([q, a]) => (
          <div key={q}>
            <dt className="text-xs font-medium text-fg-muted">{q}</dt>
            <dd className="whitespace-pre-wrap text-sm">{a}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

function SendSurvey({
  guildId,
  topics,
}: {
  guildId: string;
  topics: Pick<Topic, "name" | "label">[];
}) {
  const toast = useToast();
  const [name, setName] = React.useState(topics[0]?.name ?? "");
  const [roleIds, setRoleIds] = React.useState<string[]>([]);
  const [userIds, setUserIds] = React.useState<string[]>([]);
  const [busy, setBusy] = React.useState(false);

  if (topics.length === 0) return null;

  const send = async () => {
    setBusy(true);
    const result = await sendSurvey(guildId, name, roleIds, userIds);
    setBusy(false);
    if (result.ok) {
      toast.success("Survey sent", "It's on its way to everyone you picked.");
      setRoleIds([]);
      setUserIds([]);
    } else {
      toast.error("Couldn't send", result.error ?? "Try again.");
    }
  };

  return (
    <Section
      title="Send a survey"
      description="DM a survey's questions to members so they can fill it in."
    >
      <SelectField
        label="Which survey"
        value={name}
        onChange={setName}
        options={topics.map((t) => ({ value: t.name, label: t.label, description: t.name }))}
      />
      <div className="hidden sm:block" />
      <RolesField
        label="Send to roles"
        hint="Every member with one of these roles gets a DM."
        value={roleIds}
        onChange={setRoleIds}
      />
      <IdListField
        label="…and/or specific members"
        hint="Optional. Discord user IDs, one per line."
        value={userIds}
        onChange={setUserIds}
      />
      <div className="sm:col-span-2">
        <Button variant="primary" onClick={send} loading={busy}>
          <Send aria-hidden />
          Send survey
        </Button>
      </div>
    </Section>
  );
}
