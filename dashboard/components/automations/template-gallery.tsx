"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { ArrowRight, Search } from "lucide-react";

import { Plus } from "lucide-react";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { Input } from "@/components/ui/input";
import { Sheet } from "@/components/ui/sheet";
import { useToast } from "@/components/ui/toast";
import { Reveal } from "@/components/ui/reveal";
import { templatesByCategory, type Template } from "@/lib/automations/templates";
import { TRIGGERS } from "@/lib/automations/registry";
import { createBlank, createFromTemplate } from "@/app/actions/automations";

/**
 * The ready-made automations, as the way in.
 *
 * "Create new" sits at the top as an explicit escape hatch, but the templates
 * below it are still the intended path. Starting blank means understanding the
 * whole model before you can build anything — which trigger, which checks,
 * which steps, in what order — and almost nobody actually wants to; they want
 * a welcome message. Reading one that already works and changing it is a far
 * shorter path to understanding how the thing behaves.
 *
 * Every card says how many questions it will ask, because the difference
 * between a one-question setup and a four-question one is worth knowing
 * *before* you commit to it.
 */
export function TemplateGallery({ guildId }: { guildId: string }) {
  const router = useRouter();
  const { error } = useToast();
  const [query, setQuery] = React.useState("");
  const [pending, setPending] = React.useState<string | null>(null);
  const [blankOpen, setBlankOpen] = React.useState(false);

  const groups = React.useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return templatesByCategory();
    return templatesByCategory()
      .map(([category, templates]) => {
        const matched = templates.filter((template) =>
          `${template.name} ${template.blurb} ${category}`.toLowerCase().includes(needle),
        );
        return [category, matched] as [string, Template[]];
      })
      .filter(([, templates]) => templates.length > 0);
  }, [query]);

  const start = async (template: Template) => {
    setPending(template.key);
    const result = await createFromTemplate(guildId, template.key);
    if (!result.ok || !result.id) {
      setPending(null);
      error("Couldn't start that", result.error);
      return;
    }
    // Straight into setup rather than back to the list: the automation exists
    // now but does nothing useful yet, and dropping someone on a list with a
    // half-finished row is how it stays half-finished.
    router.push(`/dashboard/${guildId}/automations/${result.id}?setup=1`);
  };

  const total = groups.reduce((sum, [, templates]) => sum + templates.length, 0);

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <p className="text-sm text-fg-muted">
          Start from a blank automation, or pick one of the ready-made ones below.
        </p>
        <Button onClick={() => setBlankOpen(true)} disabled={pending !== null}>
          <Plus aria-hidden />
          Create new
        </Button>
      </div>

      <div className="space-y-3">
        <h2 className="text-sm font-semibold text-fg-muted">Templates to start from</h2>
        <div className="relative">
          <Search
            className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-fg-subtle"
            aria-hidden
          />
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search — try “welcome”, “role”, “delete”"
            className="pl-9"
            aria-label="Search ready-made automations"
          />
        </div>
      </div>

      {total === 0 ? (
        <Card className="p-8 text-center">
          <p className="text-sm text-fg-muted">
            Nothing matches “{query}”. You can still{" "}
            <button
              type="button"
              onClick={() => setBlankOpen(true)}
              className="font-medium text-[var(--accent)] underline underline-offset-2"
            >
              build one from scratch
            </button>
            .
          </p>
        </Card>
      ) : (
        groups.map(([category, templates], groupIndex) => (
          <section key={category} className="space-y-3">
            <h2 className="text-sm font-semibold text-fg-muted">{category}</h2>
            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
              {templates.map((template, index) => (
                <Reveal key={template.key} delay={groupIndex * 2 + index} className="min-w-0">
                  <TemplateCard
                    template={template}
                    pending={pending === template.key}
                    disabled={pending !== null}
                    onStart={() => start(template)}
                  />
                </Reveal>
              ))}
            </div>
          </section>
        ))
      )}

      <BlankSheet
        open={blankOpen}
        onOpenChange={setBlankOpen}
        guildId={guildId}
        onCreated={(id) => router.push(`/dashboard/${guildId}/automations/${id}`)}
      />
    </div>
  );
}

function TemplateCard({
  template,
  pending,
  disabled,
  onStart,
}: {
  template: Template;
  pending: boolean;
  disabled: boolean;
  onStart: () => void;
}) {
  const trigger = TRIGGERS[template.triggerType];

  return (
    <Card
      className={cn(
        "group relative flex h-full flex-col gap-3 p-5 transition-all",
        "hover:-translate-y-0.5 hover:border-[var(--border-strong)] hover:shadow-[var(--elev-2)]",
        disabled && !pending && "opacity-50",
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <span
          className="grid size-10 shrink-0 place-items-center rounded-lg bg-[var(--accent-soft)] text-[var(--accent)]"
          aria-hidden
        >
          <Icon name={template.icon} className="size-5" />
        </span>
        <span className="rounded-full bg-[var(--surface-active)] px-2 py-0.5 text-[11px] text-fg-subtle">
          {template.questions === 1 ? "1 question" : `${template.questions} questions`}
        </span>
      </div>

      <div className="min-w-0 flex-1">
        <h3 className="font-semibold tracking-tight">{template.name}</h3>
        <p className="mt-1 text-sm text-fg-muted">{template.blurb}</p>
      </div>

      {trigger && (
        <p className="text-xs text-fg-subtle">
          Runs when <span className="text-fg-muted">{trigger.label.toLowerCase()}</span>
        </p>
      )}

      <Button
        onClick={onStart}
        disabled={disabled}
        variant="secondary"
        className="w-full justify-center"
      >
        {pending ? "Setting it up…" : "Use this"}
        {!pending && <ArrowRight aria-hidden />}
      </Button>
    </Card>
  );
}

function BlankSheet({
  open,
  onOpenChange,
  guildId,
  onCreated,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  guildId: string;
  onCreated: (id: string) => void;
}) {
  const { error } = useToast();
  const [name, setName] = React.useState("");
  const [pending, setPending] = React.useState(false);

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    setPending(true);
    const result = await createBlank(guildId, name);
    setPending(false);
    if (!result.ok || !result.id) {
      error("Couldn't create it", result.error);
      return;
    }
    onCreated(result.id);
  };

  return (
    <Sheet
      open={open}
      onOpenChange={onOpenChange}
      title="Build from scratch"
      description="You'll pick what sets it off and what it does on the next screen."
    >
      <form onSubmit={submit} className="space-y-4">
        <div className="space-y-2">
          <label htmlFor="new-automation-name" className="text-sm font-medium">
            What should this one be called?
          </label>
          <Input
            id="new-automation-name"
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="Welcome message"
            maxLength={100}
            autoFocus
          />
          <p className="text-xs text-fg-subtle">
            Only you see this. Something you&apos;ll recognise in a month is the right answer.
          </p>
        </div>
        <Button type="submit" disabled={pending || !name.trim()} className="w-full justify-center">
          {pending ? "Creating…" : "Create it"}
        </Button>
      </form>
    </Sheet>
  );
}
