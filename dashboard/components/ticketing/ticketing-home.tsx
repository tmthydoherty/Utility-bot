"use client";

import * as React from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { ArrowRight, LayoutPanelTop, Plus, Send, Sparkles } from "lucide-react";

import { cn } from "@/lib/utils";
import type { Panel, Topic } from "@/lib/ticketing/types";
import { slugify } from "@/lib/ticketing/types";
import { TOPIC_TEMPLATES } from "@/lib/ticketing/templates";
import { createPanel, createTopic, publishPanel } from "@/app/actions/ticketing";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Icon } from "@/components/ui/icon";
import { EmptyState } from "@/components/ui/empty-state";
import { useToast } from "@/components/ui/toast";

const TYPE_LABEL: Record<Topic["type"], string> = {
  ticket: "Ticket",
  application: "Application",
  survey: "Survey",
};

export function TicketingHome({
  guildId,
  topics,
  panels,
  responseCount,
}: {
  guildId: string;
  topics: Topic[];
  panels: Panel[];
  responseCount: number;
}) {
  const isEmpty = topics.length === 0 && panels.length === 0;

  return (
    <div className="space-y-8">
      {isEmpty ? (
        <FirstRun guildId={guildId} />
      ) : (
        <>
          <Creators guildId={guildId} />

          <Board title="Panels" icon="LayoutPanelTop" count={panels.length}>
            {panels.length === 0 ? (
              <EmptyRow text="No panels yet. A panel is the message members click to open a ticket." />
            ) : (
              <div className="grid gap-3 lg:grid-cols-2">
                {panels.map((p) => (
                  <PanelCard key={p.name} guildId={guildId} panel={p} />
                ))}
              </div>
            )}
          </Board>

          <Board title="Topics" icon="Tag" count={topics.length}>
            {topics.length === 0 ? (
              <EmptyRow text="No topics yet. A topic is one reason someone opens a ticket." />
            ) : (
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                {topics.map((t) => (
                  <TopicCard key={t.name} guildId={guildId} topic={t} />
                ))}
              </div>
            )}
          </Board>

          <Link
            href={`/dashboard/${guildId}/ticketing/responses`}
            className="glass flex items-center gap-3 rounded-xl p-4 transition-colors hover:bg-[var(--surface-hover)]"
          >
            <Send className="size-5 text-fg-muted" aria-hidden />
            <div className="min-w-0 flex-1">
              <p className="font-medium">Responses</p>
              <p className="text-sm text-fg-muted">
                {responseCount === 0
                  ? "Application and survey answers, and send a survey out."
                  : `${responseCount.toLocaleString()} stored — view, export, or send a survey.`}
              </p>
            </div>
            <ArrowRight className="size-4 text-fg-subtle" aria-hidden />
          </Link>
        </>
      )}
    </div>
  );
}

function Board({
  title,
  icon,
  count,
  children,
}: {
  title: string;
  icon: string;
  count: number;
  children: React.ReactNode;
}) {
  return (
    <section className="space-y-3">
      <div className="flex items-center gap-2">
        <Icon name={icon} className="size-4 text-fg-muted" />
        <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">{title}</h2>
        <Badge variant="neutral">{count}</Badge>
      </div>
      {children}
    </section>
  );
}

function EmptyRow({ text }: { text: string }) {
  return (
    <p className="rounded-xl border border-dashed border-[var(--border)] p-6 text-center text-sm text-fg-muted">
      {text}
    </p>
  );
}

function Creators({ guildId }: { guildId: string }) {
  const [mode, setMode] = React.useState<null | "topic" | "panel">(null);
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-2">
        <Button variant={mode === "topic" ? "primary" : "secondary"} onClick={() => setMode(mode === "topic" ? null : "topic")}>
          <Plus aria-hidden />
          New topic
        </Button>
        <Button variant={mode === "panel" ? "primary" : "secondary"} onClick={() => setMode(mode === "panel" ? null : "panel")}>
          <Plus aria-hidden />
          New panel
        </Button>
      </div>
      {mode === "topic" && <TopicCreator guildId={guildId} onDone={() => setMode(null)} />}
      {mode === "panel" && <PanelCreator guildId={guildId} onDone={() => setMode(null)} />}
    </div>
  );
}

function TopicCreator({ guildId, onDone }: { guildId: string; onDone: () => void }) {
  const router = useRouter();
  const toast = useToast();
  const [templateKey, setTemplateKey] = React.useState<string>("blank");
  const [name, setName] = React.useState("");
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const pick = (key: string, suggested: string) => {
    setTemplateKey(key);
    if (!name) setName(suggested);
  };

  const create = async () => {
    const slug = slugify(name);
    if (!slug) {
      setError("Give the topic a short name — letters, numbers and dashes.");
      return;
    }
    setBusy(true);
    setError(null);
    const result = await createTopic(guildId, slug, templateKey === "blank" ? undefined : templateKey);
    setBusy(false);
    if (result.ok && result.name) {
      toast.success("Topic created", "Now set where its tickets go.");
      router.push(`/dashboard/${guildId}/ticketing/topics/${result.name}`);
      onDone();
    } else {
      setError(result.error ?? "Couldn't create that.");
    }
  };

  return (
    <div className="glass space-y-4 rounded-xl p-5">
      <p className="text-sm font-medium">Start from a ready-made topic, or a blank one.</p>
      <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        <TemplateChip
          active={templateKey === "blank"}
          icon="FilePlus2"
          title="Blank"
          blurb="Set everything up yourself."
          onClick={() => pick("blank", "")}
        />
        {TOPIC_TEMPLATES.map((t) => (
          <TemplateChip
            key={t.key}
            active={templateKey === t.key}
            icon={t.icon}
            title={t.label}
            blurb={t.blurb}
            onClick={() => pick(t.key, t.name)}
          />
        ))}
      </div>
      <div className="flex flex-wrap items-end gap-3">
        <div className="min-w-[16rem] flex-1 space-y-1.5">
          <label className="text-sm font-medium text-fg-muted" htmlFor="new-topic-name">
            Short name
          </label>
          <Input
            id="new-topic-name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g. support"
          />
        </div>
        <Button variant="primary" onClick={create} loading={busy}>
          Create topic
        </Button>
      </div>
      {error && <p className="text-sm text-[var(--danger)]">{error}</p>}
    </div>
  );
}

function PanelCreator({ guildId, onDone }: { guildId: string; onDone: () => void }) {
  const router = useRouter();
  const toast = useToast();
  const [name, setName] = React.useState("");
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const create = async () => {
    const slug = slugify(name);
    if (!slug) {
      setError("Give the panel a short name — letters, numbers and dashes.");
      return;
    }
    setBusy(true);
    setError(null);
    const result = await createPanel(guildId, slug);
    setBusy(false);
    if (result.ok && result.name) {
      toast.success("Panel created", "Now add topics and publish it.");
      router.push(`/dashboard/${guildId}/ticketing/panels/${result.name}`);
      onDone();
    } else {
      setError(result.error ?? "Couldn't create that.");
    }
  };

  return (
    <div className="glass space-y-3 rounded-xl p-5">
      <div className="flex flex-wrap items-end gap-3">
        <div className="min-w-[16rem] flex-1 space-y-1.5">
          <label className="text-sm font-medium text-fg-muted" htmlFor="new-panel-name">
            Short name
          </label>
          <Input
            id="new-panel-name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g. main-support"
          />
        </div>
        <Button variant="primary" onClick={create} loading={busy}>
          Create panel
        </Button>
      </div>
      {error && <p className="text-sm text-[var(--danger)]">{error}</p>}
    </div>
  );
}

function TemplateChip({
  active,
  icon,
  title,
  blurb,
  onClick,
}: {
  active: boolean;
  icon: string;
  title: string;
  blurb: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "rounded-xl border p-3 text-left transition-colors",
        active
          ? "border-[var(--accent)] bg-[var(--accent-soft)]"
          : "border-[var(--border)] bg-[var(--surface)] hover:bg-[var(--surface-hover)]",
      )}
    >
      <Icon name={icon} className={cn("size-5", active ? "text-[var(--accent)]" : "text-fg-muted")} />
      <p className="mt-2 text-sm font-medium">{title}</p>
      <p className="text-xs text-fg-muted">{blurb}</p>
    </button>
  );
}

function TopicCard({ guildId, topic }: { guildId: string; topic: Topic }) {
  return (
    <Link
      href={`/dashboard/${guildId}/ticketing/topics/${topic.name}`}
      className="glass flex flex-col gap-2 rounded-xl p-4 transition-colors hover:bg-[var(--surface-hover)]"
    >
      <div className="flex items-center gap-2">
        <span className="truncate font-medium">
          {topic.emoji ? `${topic.emoji} ` : ""}
          {topic.label}
        </span>
      </div>
      <div className="flex flex-wrap items-center gap-1.5">
        <Badge variant="neutral">{TYPE_LABEL[topic.type]}</Badge>
        <Badge variant="outline">{topic.mode === "channel" ? "Channel" : "Thread"}</Badge>
        {topic.questions.length > 0 && <Badge variant="outline">{topic.questions.length} Qs</Badge>}
      </div>
      {topic.staffRoleIds.length === 0 && (
        <p className="text-xs text-[var(--warning)]">No staff roles set</p>
      )}
    </Link>
  );
}

function PanelCard({ guildId, panel }: { guildId: string; panel: Panel }) {
  const router = useRouter();
  const toast = useToast();
  const [busy, setBusy] = React.useState(false);
  const published = Boolean(panel.messageId);

  const quickPublish = async (e: React.MouseEvent) => {
    e.preventDefault();
    setBusy(true);
    const result = await publishPanel(guildId, panel.name);
    setBusy(false);
    if (result.ok) {
      toast.success(published ? "Panel updated" : "Panel published", "Live within about 10 seconds.");
      router.refresh();
    } else {
      toast.error("Couldn't publish", result.error ?? "Open the panel to fix it.");
    }
  };

  return (
    <Link
      href={`/dashboard/${guildId}/ticketing/panels/${panel.name}`}
      className="glass flex flex-col gap-3 rounded-xl p-4 transition-colors hover:bg-[var(--surface-hover)]"
    >
      <div className="flex items-center gap-2">
        <LayoutPanelTop className="size-4 shrink-0 text-fg-muted" aria-hidden />
        <span className="truncate font-medium">{panel.title || panel.name}</span>
        {published ? (
          <Badge variant="success" className="ml-auto">Published</Badge>
        ) : (
          <Badge variant="warning" className="ml-auto">Draft</Badge>
        )}
      </div>
      <p className="text-sm text-fg-muted">
        {panel.topicNames.length} topic{panel.topicNames.length === 1 ? "" : "s"}
        {panel.categories.length > 0 && ` · ${panel.categories.length} group${panel.categories.length === 1 ? "" : "s"}`}
      </p>
      <div className="flex gap-2">
        <Button variant="secondary" size="sm" onClick={quickPublish} loading={busy} disabled={!panel.channelId || panel.topicNames.length === 0}>
          <Send aria-hidden />
          {published ? "Update" : "Publish"}
        </Button>
        <span className="ml-auto flex items-center text-xs text-fg-subtle">Edit<ArrowRight className="ml-1 size-3" aria-hidden /></span>
      </div>
    </Link>
  );
}

function FirstRun({ guildId }: { guildId: string }) {
  return (
    <div className="space-y-6">
      <EmptyState
        icon="TicketCheck"
        title="Set up ticketing"
        description="Two pieces: a topic (a reason someone opens a ticket) and a panel (the message they click). Start with a ready-made topic below — you can change anything after."
      />
      <div className="flex items-center gap-2">
        <Sparkles className="size-4 text-fg-muted" aria-hidden />
        <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">Start with a topic</h2>
      </div>
      <TopicCreator guildId={guildId} onDone={() => {}} />
    </div>
  );
}
