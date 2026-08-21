"use client";

import * as React from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { ArrowLeft, Check, Pencil, TriangleAlert, Wand2, Wrench } from "lucide-react";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { SaveBar } from "@/components/ui/save-bar";
import { useToast } from "@/components/ui/toast";
import { blanks, destructiveSteps, isReady, requiredPermissions } from "@/lib/automations/readiness";
import { permissionName } from "@/lib/automations/registry";
import { stateOf, type Automation, type AutomationState } from "@/lib/automations/types";
import { TEMPLATES_BY_KEY } from "@/lib/automations/templates";
import * as actions from "@/app/actions/automations";

import { AdvancedSection } from "./advanced";
import { AutomationSentence } from "./sentence";
import { ConditionsSection, StepsSection, TriggerSection } from "./sections";
import { SetupFlow } from "./setup-flow";
import { StateControl } from "./state-control";

/**
 * The automation builder.
 *
 * The sentence is at the top and everything else is underneath it. That
 * ordering is the whole design: the sentence is what the automation *does*,
 * and the sections are only how it is spelled. Someone who reads the sentence
 * and agrees with it is finished, whether or not they understood a single one
 * of the three cards below.
 *
 * Edits are held locally and committed with one Save. That is the opposite of
 * the Discord panel, which writes every change immediately — right there,
 * because a draft held in a Discord view is lost when the panel times out.
 * A browser tab does not time out, an automation is a sentence you want to
 * finish before it takes effect, and a page that saved on every keystroke
 * would have the engine reloading its cache once per letter typed.
 */
export function AutomationBuilder({
  automation: initial,
  guildId,
  botPermissions,
}: {
  automation: Automation;
  guildId: string;
  /** What the bot actually holds in this guild, for the permission warning. */
  botPermissions: string[];
}) {
  const router = useRouter();
  const params = useSearchParams();
  const { toast, error: toastError } = useToast();

  const [draft, setDraft] = React.useState<Automation>(initial);
  const [saving, setSaving] = React.useState(false);
  const [saveError, setSaveError] = React.useState<string | null>(null);
  const [renaming, setRenaming] = React.useState(false);
  const [setupOpen, setSetupOpen] = React.useState(params.get("setup") === "1");

  // A save round-trips through the server and comes back as a fresh prop; that
  // is the moment the local draft is known to match the database again.
  React.useEffect(() => {
    setDraft(initial);
  }, [initial]);

  const dirty = React.useMemo(() => !sameAutomation(draft, initial), [draft, initial]);
  const remaining = React.useMemo(() => blanks(draft), [draft]);
  const ready = React.useMemo(() => isReady(draft), [draft]);
  const state = stateOf(draft);
  const template = draft.templateKey ? TEMPLATES_BY_KEY[draft.templateKey] : undefined;

  const missingPerms = React.useMemo(
    () => requiredPermissions(draft).filter((perm) => !botPermissions.includes(perm)),
    [draft, botPermissions],
  );
  const destructive = React.useMemo(() => destructiveSteps(draft), [draft]);

  const patch = (changes: Partial<Automation>) =>
    setDraft((current) => ({ ...current, ...changes }));

  const save = async () => {
    setSaving(true);
    setSaveError(null);
    const result = await actions.save(
      guildId,
      draft.id,
      {
        name: draft.name,
        triggerType: draft.triggerType,
        triggerConfig: draft.triggerConfig,
        conditions: draft.conditions,
        steps: draft.steps,
        priority: draft.priority,
        stopAfter: draft.stopAfter,
        cooldownSeconds: draft.cooldownSeconds,
        cooldownScope: draft.cooldownScope,
        allowBots: draft.allowBots,
      },
      draft.name,
    );
    setSaving(false);

    if (!result.ok) {
      setSaveError(result.error ?? "Something went wrong.");
      return;
    }
    toast({
      variant: "success",
      title: "Saved",
      description: draft.enabled ? "Vibey picks this up within about 10 seconds." : undefined,
    });
    router.refresh();
  };

  const changeState = async (next: AutomationState) => {
    if (dirty) {
      toastError("Save first", "There are unsaved changes — save them, then switch it on.");
      return;
    }
    const result = await actions.setState(guildId, draft.id, next);
    if (!result.ok) {
      toastError("Couldn't change that", result.error);
      return;
    }
    toast({
      variant: "success",
      title: next === "off" ? "Switched off" : next === "testing" ? "Now in test mode" : "Switched on",
      description: next === "off" ? undefined : "Vibey picks this up within about 10 seconds.",
    });
    router.refresh();
  };

  return (
    // No bottom padding here — the page owns it, so the run history that
    // follows sits in the same rhythm as these cards rather than after a
    // seven-rem hole.
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3">
        <Button asChild variant="ghost" size="sm">
          <Link href={`/dashboard/${guildId}/automations`}>
            <ArrowLeft aria-hidden />
            All automations
          </Link>
        </Button>
      </div>

      {/* ---- the headline ---- */}
      <Card className="space-y-4 p-5 sm:p-6">
        {/* Stacked on a phone rather than wrapped. `flex-wrap` with a flexible
            title and a fixed-width control looks fine until the name is long,
            at which point the title refuses to shrink and the two collide. A
            column below `sm` has no such failure mode. */}
        <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div className="min-w-0 flex-1">
            {renaming ? (
              <Input
                value={draft.name}
                onChange={(event) => patch({ name: event.target.value })}
                onBlur={() => setRenaming(false)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === "Escape") setRenaming(false);
                }}
                maxLength={100}
                autoFocus
                aria-label="Automation name"
                className="max-w-sm"
              />
            ) : (
              <button
                type="button"
                onClick={() => setRenaming(true)}
                className="group flex items-center gap-2 text-left"
              >
                <h1 className="truncate text-xl font-semibold tracking-tight">{draft.name}</h1>
                <Pencil
                  className="size-3.5 shrink-0 text-fg-subtle opacity-0 transition-opacity group-hover:opacity-100"
                  aria-hidden
                />
              </button>
            )}
            <p className="mt-0.5 text-xs text-fg-subtle">
              {draft.runCount > 0
                ? `Has run ${draft.runCount.toLocaleString()} time${draft.runCount === 1 ? "" : "s"}`
                : "Hasn't run yet"}
            </p>
          </div>

          <StateControl
            className="shrink-0"
            value={state}
            onChange={changeState}
            blockedReason={
              !ready
                ? `${remaining.length} thing${remaining.length === 1 ? "" : "s"} still to fill in`
                : dirty
                  ? "Save your changes first"
                  : undefined
            }
          />
        </div>

        <div className="rounded-lg border border-[var(--border)] bg-[var(--bg-inset)] p-4">
          <p className="mb-1.5 text-xs font-medium uppercase tracking-wide text-fg-subtle">
            In plain English
          </p>
          <AutomationSentence automation={draft} />
        </div>

        {remaining.length > 0 && (
          <div className="flex flex-col gap-3 rounded-lg border border-[var(--warning)] bg-[var(--warning-soft)] p-4 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-start gap-2.5">
              <Wrench className="mt-0.5 size-4 shrink-0 text-[var(--warning)]" aria-hidden />
              <div>
                <p className="text-sm font-medium">
                  {remaining.length} thing{remaining.length === 1 ? "" : "s"} still to fill in
                </p>
                <p className="text-sm text-fg-muted">
                  It can&apos;t be switched on until {remaining.length === 1 ? "it's" : "they're"} done.
                </p>
              </div>
            </div>
            <Button onClick={() => setSetupOpen(true)} className="shrink-0">
              <Wand2 aria-hidden />
              Walk me through it
            </Button>
          </div>
        )}

        {missingPerms.length > 0 && (
          <Warning
            title="Vibey is missing a permission"
            body={`It needs ${missingPerms.map(permissionName).join(" and ")} in Discord for the steps below to work. Add it in Server Settings → Roles.`}
          />
        )}

        {destructive.length > 0 && state !== "live" && (
          <Warning
            title="This one takes things away"
            body={`It can ${destructive.map((label) => label.toLowerCase()).join(" and ")}. Test mode is a good idea before switching it on properly.`}
            tone="info"
          />
        )}
      </Card>

      {/* ---- the three questions ---- */}
      <TriggerSection
        triggerType={draft.triggerType}
        config={draft.triggerConfig}
        onChangeType={(triggerType) =>
          // Checks and steps can depend on what the trigger provides, so
          // changing it clears the trigger's own settings — but not the rest,
          // because throwing away someone's message text because they picked
          // the wrong event first would be far worse than one stale check.
          patch({ triggerType, triggerConfig: {} })
        }
        onChangeConfig={(triggerConfig) => patch({ triggerConfig })}
      />

      <ConditionsSection
        triggerType={draft.triggerType}
        conditions={draft.conditions}
        onChange={(conditions) => patch({ conditions })}
      />

      <StepsSection
        triggerType={draft.triggerType}
        steps={draft.steps}
        onChange={(steps) => patch({ steps })}
      />

      <AdvancedSection
        values={{
          cooldownSeconds: draft.cooldownSeconds,
          cooldownScope: draft.cooldownScope,
          priority: draft.priority,
          stopAfter: draft.stopAfter,
          allowBots: draft.allowBots,
        }}
        onChange={(next) => patch(next)}
      />

      {template?.needs && template.needs.length > 0 && (
        <Card className="space-y-2 p-5 sm:p-6">
          <h2 className="font-semibold tracking-tight">Worth knowing</h2>
          <ul className="space-y-2">
            {template.needs.map((tip) => (
              <li key={tip} className="flex items-start gap-2.5 text-sm text-fg-muted">
                <Check className="mt-0.5 size-4 shrink-0 text-fg-subtle" aria-hidden />
                {tip}
              </li>
            ))}
          </ul>
        </Card>
      )}

      <SaveBar
        visible={dirty}
        saving={saving}
        error={saveError}
        changeCount={1}
        onSave={save}
        onReset={() => {
          setDraft(initial);
          setSaveError(null);
        }}
      />

      <SetupFlow
        open={setupOpen}
        onOpenChange={setSetupOpen}
        automation={draft}
        onChange={setDraft}
        onFinish={save}
      />
    </div>
  );
}

function Warning({
  title,
  body,
  tone = "warning",
}: {
  title: string;
  body: string;
  tone?: "warning" | "info";
}) {
  return (
    <div
      className={cn(
        "flex items-start gap-2.5 rounded-lg border p-4",
        tone === "warning"
          ? "border-[var(--warning)] bg-[var(--warning-soft)]"
          : "border-[var(--border)] bg-[var(--bg-inset)]",
      )}
    >
      <TriangleAlert
        className={cn(
          "mt-0.5 size-4 shrink-0",
          tone === "warning" ? "text-[var(--warning)]" : "text-fg-subtle",
        )}
        aria-hidden
      />
      <div>
        <p className="text-sm font-medium">{title}</p>
        <p className="text-sm text-fg-muted">{body}</p>
      </div>
    </div>
  );
}

/**
 * Whether the draft still matches what was loaded.
 *
 * Compared by serialising rather than field by field, because the interesting
 * parts are two nested trees and a hand-written comparison of those is how a
 * Save button ends up permanently lit or permanently dead. `enabled` and
 * `dryRun` are excluded on purpose: those are changed by their own control,
 * which saves immediately, and including them would light the Save bar the
 * instant someone switched the automation on.
 */
function sameAutomation(a: Automation, b: Automation): boolean {
  const shape = (automation: Automation) =>
    JSON.stringify({
      name: automation.name,
      triggerType: automation.triggerType,
      triggerConfig: automation.triggerConfig,
      conditions: automation.conditions,
      steps: automation.steps,
      priority: automation.priority,
      stopAfter: automation.stopAfter,
      cooldownSeconds: automation.cooldownSeconds,
      cooldownScope: automation.cooldownScope,
      allowBots: automation.allowBots,
    });
  return shape(a) === shape(b);
}
