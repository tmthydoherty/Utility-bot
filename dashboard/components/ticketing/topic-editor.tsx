"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Trash2 } from "lucide-react";

import { ChannelType } from "@/lib/discord/types";
import type { ButtonColor, Topic, TopicType } from "@/lib/ticketing/types";
import { updateTopic, deleteTopic } from "@/app/actions/ticketing";
import { Button } from "@/components/ui/button";
import { SaveBar } from "@/components/ui/save-bar";
import { useToast } from "@/components/ui/toast";
import {
  Section,
  TextField,
  TextAreaField,
  NumberField,
  Toggle,
  SelectField,
  ChannelField,
  CategoryField,
  RoleField,
  RolesField,
  QuestionsEditor,
  IdListField,
} from "./controls";

const TYPE_OPTIONS: { value: TopicType; label: string; description: string }[] = [
  { value: "ticket", label: "Ticket", description: "A private channel or thread staff and the member talk in." },
  { value: "application", label: "Application", description: "Questions the member answers, sent to staff to review." },
  { value: "survey", label: "Survey", description: "Questions whose answers are stored — no channel opens." },
];

const COLOR_OPTIONS: { value: ButtonColor; label: string }[] = [
  { value: "primary", label: "Blurple" },
  { value: "secondary", label: "Grey" },
  { value: "success", label: "Green" },
  { value: "danger", label: "Red" },
];

const TEXT_CHANNELS = [ChannelType.GuildText, ChannelType.GuildAnnouncement];

export function TopicEditor({ guildId, topic }: { guildId: string; topic: Topic }) {
  const router = useRouter();
  const toast = useToast();

  const [saved, setSaved] = React.useState(topic);
  const [values, setValues] = React.useState(topic);
  const [saving, setSaving] = React.useState(false);
  const [saveError, setSaveError] = React.useState<string | null>(null);
  const [deleting, setDeleting] = React.useState(false);

  const changedCount = (Object.keys(values) as (keyof Topic)[]).filter(
    (k) => JSON.stringify(values[k]) !== JSON.stringify(saved[k]),
  ).length;
  const dirty = changedCount > 0;
  const set = <K extends keyof Topic>(key: K, value: Topic[K]) =>
    setValues((v) => ({ ...v, [key]: value }));

  React.useEffect(() => {
    if (!dirty) return;
    const warn = (e: BeforeUnloadEvent) => e.preventDefault();
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [dirty]);

  const save = async () => {
    setSaving(true);
    setSaveError(null);
    try {
      const result = await updateTopic(guildId, values);
      if (result.ok) {
        setSaved(values);
        toast.success("Topic saved", `${values.label} updated. Live within about 10 seconds.`);
      } else {
        setSaveError(result.error ?? "Something went wrong.");
      }
    } catch {
      setSaveError("Couldn't reach the server. Your changes are still here.");
    } finally {
      setSaving(false);
    }
  };

  const remove = async () => {
    if (!confirm(`Delete the "${values.label}" topic? This can't be undone, and it will be removed from any panels.`)) return;
    setDeleting(true);
    const result = await deleteTopic(guildId, values.name);
    if (result.ok) {
      toast.success("Topic deleted", `${values.label} removed.`);
      router.push(`/dashboard/${guildId}/ticketing`);
    } else {
      toast.error("Couldn't delete", result.error ?? "Try again.");
      setDeleting(false);
    }
  };

  const collectsAnswers = values.type !== "ticket" || values.questions.length > 0;

  return (
    <div className="space-y-5 pb-28 lg:pb-24">
      <Section title="Basics" description="What this topic is and where its tickets go.">
        <TextField
          label="Button label"
          hint="What members see on the panel — e.g. “Get Support”."
          value={values.label}
          onChange={(v) => set("label", v)}
          maxLength={80}
        />
        <TextField
          label="Emoji"
          hint="Optional. Shown before the label on the panel."
          value={values.emoji ?? ""}
          onChange={(v) => set("emoji", v || null)}
          placeholder="🎫"
        />
        <SelectField
          label="Kind"
          hint="Changes how the topic behaves, not just its wording."
          value={values.type}
          onChange={(v) => set("type", v)}
          options={TYPE_OPTIONS}
        />
        <SelectField
          label="Ticket location"
          hint="A thread keeps the parent channel tidy; a channel gives each ticket its own place under a category."
          value={values.mode}
          onChange={(v) => set("mode", v)}
          options={[
            { value: "thread", label: "Private thread" },
            { value: "channel", label: "Channel under a category" },
          ]}
        />
        {values.mode === "channel" ? (
          <CategoryField
            label="Category tickets open under"
            hint="New ticket channels are created here."
            value={values.parentId}
            onChange={(v) => set("parentId", v)}
            wide
          />
        ) : (
          <ChannelField
            label="Channel threads open in"
            hint="Private threads are created under this channel."
            value={values.parentId}
            onChange={(v) => set("parentId", v)}
            types={TEXT_CHANNELS}
            wide
          />
        )}
        <RolesField
          label="Staff roles"
          hint="These roles can see and reply to every ticket of this topic."
          value={values.staffRoleIds}
          onChange={(v) => set("staffRoleIds", v)}
        />
        <SelectField
          label="Button colour"
          value={values.buttonColor}
          onChange={(v) => set("buttonColor", v)}
          options={COLOR_OPTIONS}
        />
        <Toggle
          label="Ping staff when a ticket opens"
          hint="Sends a quick mention to the staff roles in the new ticket."
          checked={values.pingStaffOnCreate}
          onChange={(v) => set("pingStaffOnCreate", v)}
        />
      </Section>

      <Section title="Messages" description="What people see when a ticket opens and closes.">
        <TextAreaField
          label="Welcome message"
          hint="Shown at the top of a new ticket. Use {user} for the member and {topic} for this topic's label."
          value={values.welcomeMessage}
          onChange={(v) => set("welcomeMessage", v)}
          rows={3}
        />
        <TextAreaField
          label="Close message (DM'd to the opener)"
          hint="Use {channel}, {server} and {closer}. Leave as-is if you're not sure."
          value={values.closeMessage}
          onChange={(v) => set("closeMessage", v)}
          rows={2}
        />
      </Section>

      <Section
        title="Questions"
        description={
          values.type === "survey"
            ? "The survey is these questions, asked one at a time. Answers are stored under Responses."
            : values.type === "application"
              ? "Asked before the application is submitted to staff."
              : "Optional. Ask these before the ticket opens, and show the answers inside it."
        }
      >
        <QuestionsEditor value={values.questions} onChange={(v) => set("questions", v)} />
        {collectsAnswers && (
          <>
            <SelectField
              label="Where questions are asked"
              hint="In the member's DMs, or in a temporary channel."
              value={values.applicationChannelMode}
              onChange={(v) => set("applicationChannelMode", v)}
              options={[
                { value: "dm", label: "In DMs" },
                { value: "channel", label: "In a channel" },
              ]}
            />
            <ChannelField
              label="Send answers to"
              hint="Where the completed answers are posted for staff. Optional."
              value={values.logChannelId}
              onChange={(v) => set("logChannelId", v)}
              types={TEXT_CHANNELS}
            />
            <Toggle
              label="Add approve / deny buttons"
              hint="Staff can accept or reject from the posted answers."
              checked={values.approvalMode}
              onChange={(v) => set("approvalMode", v)}
            />
            <Toggle
              label="Open a discussion channel too"
              hint="As well as storing answers, open a ticket to talk it through."
              checked={values.discussionMode}
              onChange={(v) => set("discussionMode", v)}
            />
          </>
        )}
      </Section>

      <Section
        title="Requirements before opening"
        description="Ask the member to confirm they have something ready first — a profile link, a screenshot — before the ticket opens. Both are optional."
      >
        <Toggle
          label="Ask a first requirement"
          checked={values.preModalEnabled}
          onChange={(v) => set("preModalEnabled", v)}
        />
        {values.preModalEnabled && (
          <>
            <TextField
              label="The question"
              value={values.preModalQuestion}
              onChange={(v) => set("preModalQuestion", v)}
              wide
            />
            <TextField
              label="“Yes” button"
              value={values.preModalYesLabel}
              onChange={(v) => set("preModalYesLabel", v)}
            />
            <TextField
              label="“No” button"
              value={values.preModalNoLabel}
              onChange={(v) => set("preModalNoLabel", v)}
            />
            <TextAreaField
              label="Shown if they aren't ready"
              value={values.preModalNoMessage}
              onChange={(v) => set("preModalNoMessage", v)}
              rows={2}
            />
            <ChannelField
              label="Point them to a channel"
              hint="Optional. Linked in the “not ready” message."
              value={values.preModalRedirectChannelId}
              onChange={(v) => set("preModalRedirectChannelId", v)}
              types={TEXT_CHANNELS}
            />
            <TextField
              label="…or a link"
              hint="Optional. An external URL to send them to."
              value={values.preModalRedirectUrl ?? ""}
              onChange={(v) => set("preModalRedirectUrl", v || null)}
              placeholder="https://…"
            />
          </>
        )}
        <Toggle
          label="Require a typed answer"
          hint="Make them type something (a link, an ID) before the ticket opens. It's shown inside the ticket."
          checked={values.preModalAnswerEnabled}
          onChange={(v) => set("preModalAnswerEnabled", v)}
        />
        {values.preModalAnswerEnabled && (
          <TextField
            label="What to ask for"
            value={values.preModalAnswerQuestion}
            onChange={(v) => set("preModalAnswerQuestion", v)}
            wide
          />
        )}
        <Toggle
          label="Ask a second requirement"
          checked={values.preModal2Enabled}
          onChange={(v) => set("preModal2Enabled", v)}
        />
        {values.preModal2Enabled && (
          <>
            <TextField
              label="The second question"
              value={values.preModal2Question}
              onChange={(v) => set("preModal2Question", v)}
              wide
            />
            <TextField
              label="“Yes” button"
              value={values.preModal2YesLabel}
              onChange={(v) => set("preModal2YesLabel", v)}
            />
            <TextField
              label="“No” button"
              value={values.preModal2NoLabel}
              onChange={(v) => set("preModal2NoLabel", v)}
            />
            <TextAreaField
              label="Shown if they aren't ready"
              value={values.preModal2NoMessage}
              onChange={(v) => set("preModal2NoMessage", v)}
              rows={2}
            />
          </>
        )}
      </Section>

      <Section
        title="Claiming"
        description="Let one staff member take ownership of a ticket so two don't answer at once."
      >
        <Toggle
          label="Enable claiming"
          checked={values.claimEnabled}
          onChange={(v) => set("claimEnabled", v)}
        />
        {values.claimEnabled && (
          <>
            <ChannelField
              label="Post claim alerts to"
              hint="A staff channel where new tickets appear with a Claim button."
              value={values.claimAlertsChannelId}
              onChange={(v) => set("claimAlertsChannelId", v)}
              types={TEXT_CHANNELS}
            />
            <RoleField
              label="Given to the claimer"
              hint="Optional. A role added to whoever claims, for extra access."
              value={values.claimRoleId}
              onChange={(v) => set("claimRoleId", v)}
            />
          </>
        )}
      </Section>

      <Section title="Behaviour & appearance" description="The finer details.">
        <NumberField
          label="Cooldown (minutes)"
          hint="How long a member must wait between opening this topic again."
          value={values.cooldownMinutes}
          onChange={(v) => set("cooldownMinutes", v)}
          min={0}
        />
        <TextField
          label="Embed colour"
          hint="Optional hex colour, e.g. #5865F2."
          value={values.embedColor ?? ""}
          onChange={(v) => set("embedColor", v || null)}
          placeholder="#5865F2"
        />
        <Toggle
          label="Number tickets"
          hint="Names tickets like support-0001 instead of by username."
          checked={values.useNumbering}
          onChange={(v) => set("useNumbering", v)}
        />
        <TextField
          label="Custom name format"
          hint="Optional. Placeholders: {user}, {topic}, {number}."
          value={values.channelNameFormat ?? ""}
          onChange={(v) => set("channelNameFormat", v || null)}
          placeholder="{topic}-{number}"
        />
        <Toggle
          label="Delete on close"
          hint="Off keeps a closed, locked archive instead of deleting the channel."
          checked={values.deleteOnClose}
          onChange={(v) => set("deleteOnClose", v)}
        />
        <Toggle
          label="Members can close their own ticket"
          checked={values.memberCanClose}
          onChange={(v) => set("memberCanClose", v)}
        />
        <IdListField
          label="Blocked members"
          hint="User IDs that can't open this topic. One per line."
          value={values.blacklistedUserIds}
          onChange={(v) => set("blacklistedUserIds", v)}
        />
      </Section>

      <section className="glass rounded-xl border-[var(--danger)]/25 p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-sm font-semibold text-[var(--danger)]">Delete this topic</h2>
            <p className="text-sm text-fg-muted">Removes it everywhere, including from any panels. Existing open tickets are untouched.</p>
          </div>
          <Button variant="danger" onClick={remove} loading={deleting}>
            <Trash2 aria-hidden />
            Delete topic
          </Button>
        </div>
      </section>

      <SaveBar
        visible={dirty}
        saving={saving}
        error={saveError}
        changeCount={changedCount}
        onSave={save}
        onReset={() => {
          setValues(saved);
          setSaveError(null);
        }}
      />
    </div>
  );
}
