"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { ArrowDown, ArrowUp, Plus, Send, Trash2, X, EyeOff } from "lucide-react";

import { cn } from "@/lib/utils";
import { ChannelType } from "@/lib/discord/types";
import {
  MAX_PANEL_ITEMS,
  type ButtonColor,
  type Panel,
  type PanelCategory,
  type Topic,
} from "@/lib/ticketing/types";
import { updatePanel, deletePanel, publishPanel, unpublishPanel } from "@/app/actions/ticketing";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { SaveBar } from "@/components/ui/save-bar";
import { useToast } from "@/components/ui/toast";
import { EntityPicker } from "@/components/ui/entity-picker";
import {
  Section,
  Field,
  TextField,
  TextAreaField,
  SelectField,
  ChannelField,
} from "./controls";

const COLOR_CLASS: Record<ButtonColor, string> = {
  primary: "bg-[var(--accent)] text-[var(--accent-fg)]",
  secondary: "bg-[var(--surface-hover)] text-fg",
  success: "bg-[var(--success)] text-white",
  danger: "bg-[var(--danger)] text-white",
};

export function PanelBuilder({
  guildId,
  panel,
  allTopics,
}: {
  guildId: string;
  panel: Panel;
  allTopics: Topic[];
}) {
  const router = useRouter();
  const toast = useToast();

  const [saved, setSaved] = React.useState(panel);
  const [values, setValues] = React.useState(panel);
  const [saving, setSaving] = React.useState(false);
  const [publishing, setPublishing] = React.useState(false);
  const [saveError, setSaveError] = React.useState<string | null>(null);

  const topicsByName = React.useMemo(
    () => new Map(allTopics.map((t) => [t.name, t])),
    [allTopics],
  );

  const changedCount = (Object.keys(values) as (keyof Panel)[]).filter(
    (k) => JSON.stringify(values[k]) !== JSON.stringify(saved[k]),
  ).length;
  const dirty = changedCount > 0;

  React.useEffect(() => {
    if (!dirty) return;
    const warn = (e: BeforeUnloadEvent) => e.preventDefault();
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [dirty]);

  const patch = (next: Partial<Panel>) => setValues((v) => ({ ...v, ...next }));

  // ---- topic / category wiring ------------------------------------------

  const categorised = React.useMemo(() => {
    const set = new Set<string>();
    for (const c of values.categories) for (const n of c.topicNames) set.add(n);
    return set;
  }, [values.categories]);

  const availableTopics = allTopics.filter((t) => !values.topicNames.includes(t.name));

  const addTopic = (name: string) => {
    if (values.topicNames.includes(name)) return;
    patch({
      topicNames: [...values.topicNames, name],
      topicOrder: [...values.topicOrder, name],
    });
  };

  const removeTopic = (name: string) => {
    const map = { ...values.topicDisplayMap };
    delete map[name];
    patch({
      topicNames: values.topicNames.filter((n) => n !== name),
      topicOrder: values.topicOrder.filter((n) => n !== name),
      topicDisplayMap: map,
      categories: values.categories.map((c) => ({
        ...c,
        topicNames: c.topicNames.filter((n) => n !== name),
      })),
    });
  };

  const addCategory = () => {
    const base = "category";
    let slug = base;
    let i = 1;
    const taken = new Set(values.categories.map((c) => c.slug));
    while (taken.has(slug)) slug = `${base}-${++i}`;
    const cat: PanelCategory = {
      slug,
      label: "New group",
      emoji: null,
      displayMode: "buttons",
      buttonColor: "secondary",
      topicNames: [],
    };
    patch({
      categories: [...values.categories, cat],
      topicOrder: [...values.topicOrder, `cat:${slug}`],
    });
  };

  const updateCategory = (slug: string, next: Partial<PanelCategory>) =>
    patch({
      categories: values.categories.map((c) => (c.slug === slug ? { ...c, ...next } : c)),
    });

  const removeCategory = (slug: string) => {
    const cat = values.categories.find((c) => c.slug === slug);
    const freed = cat ? cat.topicNames.filter((n) => values.topicNames.includes(n)) : [];
    patch({
      categories: values.categories.filter((c) => c.slug !== slug),
      // Its topics go back to standalone, and the category leaves the order.
      topicOrder: [...values.topicOrder.filter((n) => n !== `cat:${slug}`), ...freed],
    });
  };

  const setCategoryTopics = (slug: string, topicNames: string[]) => {
    // A topic can only live in one category, and shouldn't also be standalone.
    patch({
      categories: values.categories.map((c) =>
        c.slug === slug
          ? { ...c, topicNames }
          : { ...c, topicNames: c.topicNames.filter((n) => !topicNames.includes(n)) },
      ),
      topicOrder: values.topicOrder.filter((n) => !topicNames.includes(n)),
    });
  };

  const move = (index: number, delta: number) => {
    const j = index + delta;
    if (j < 0 || j >= values.topicOrder.length) return;
    const next = [...values.topicOrder];
    [next[index], next[j]] = [next[j]!, next[index]!];
    patch({ topicOrder: next });
  };

  // Entries actually shown, in order: standalone topics + categories. Topics
  // that ended up in a category are edited inside it, not here.
  const orderedEntries = values.topicOrder
    .map((entry) => {
      if (entry.startsWith("cat:")) {
        const cat = values.categories.find((c) => c.slug === entry.slice(4));
        return cat ? ({ kind: "category", cat } as const) : null;
      }
      if (values.topicNames.includes(entry) && !categorised.has(entry)) {
        return { kind: "topic", name: entry } as const;
      }
      return null;
    })
    .filter((e): e is NonNullable<typeof e> => e !== null);

  const totalItems =
    orderedEntries.filter((e) => e.kind === "topic").length + values.categories.length;

  // ---- actions -----------------------------------------------------------

  const save = async () => {
    setSaving(true);
    setSaveError(null);
    try {
      const result = await updatePanel(guildId, values);
      if (result.ok) {
        setSaved(values);
        toast.success("Panel saved", "Publish it to update the live message.");
      } else {
        setSaveError(result.error ?? "Something went wrong.");
      }
    } catch {
      setSaveError("Couldn't reach the server. Your changes are still here.");
    } finally {
      setSaving(false);
    }
  };

  const publish = async () => {
    if (dirty) {
      const result = await updatePanel(guildId, values);
      if (!result.ok) {
        setSaveError(result.error ?? "Couldn't save before publishing.");
        return;
      }
      setSaved(values);
    }
    setPublishing(true);
    const result = await publishPanel(guildId, values.name);
    setPublishing(false);
    if (result.ok) {
      toast.success("Panel published", "The message is live within about 10 seconds.");
      router.refresh();
    } else {
      toast.error("Couldn't publish", result.error ?? "Try again.");
    }
  };

  const unpublish = async () => {
    setPublishing(true);
    const result = await unpublishPanel(guildId, values.name);
    setPublishing(false);
    if (result.ok) {
      toast.success("Panel taken down", "The live message will be removed shortly.");
      router.refresh();
    } else {
      toast.error("Couldn't take it down", result.error ?? "Try again.");
    }
  };

  const remove = async () => {
    if (!confirm(`Delete the "${values.name}" panel? Its live message will be removed too.`)) return;
    const result = await deletePanel(guildId, values.name);
    if (result.ok) {
      toast.success("Panel deleted", "");
      router.push(`/dashboard/${guildId}/ticketing`);
    } else {
      toast.error("Couldn't delete", result.error ?? "Try again.");
    }
  };

  const published = Boolean(saved.messageId);

  return (
    <div className="space-y-5 pb-28 lg:pb-24">
      {/* Publish status + actions */}
      <section className="glass flex flex-wrap items-center gap-3 rounded-xl p-4">
        {published ? (
          <Badge variant="success">Published</Badge>
        ) : (
          <Badge variant="warning">Draft — not posted yet</Badge>
        )}
        <span className="text-sm text-fg-muted">
          {published
            ? "Publishing again reposts the message with your latest changes."
            : "Members can't use this panel until you publish it to a channel."}
        </span>
        <div className="ml-auto flex gap-2">
          {published && (
            <Button variant="ghost" size="sm" onClick={unpublish} loading={publishing}>
              <EyeOff aria-hidden />
              Take down
            </Button>
          )}
          <Button variant="primary" size="sm" onClick={publish} loading={publishing}>
            <Send aria-hidden />
            {published ? "Update in Discord" : "Publish to Discord"}
          </Button>
        </div>
      </section>

      <Section title="The message" description="How the panel looks when it's posted.">
        <TextField
          label="Title"
          hint="Optional heading above the buttons."
          value={values.title ?? ""}
          onChange={(v) => patch({ title: v || null })}
          placeholder="Contact the staff"
        />
        <ChannelField
          label="Post in"
          hint="The channel the panel message lives in."
          value={values.channelId}
          onChange={(v) => patch({ channelId: v })}
          types={[ChannelType.GuildText, ChannelType.GuildAnnouncement]}
        />
        <TextAreaField
          label="Description"
          value={values.description}
          onChange={(v) => patch({ description: v })}
          rows={2}
        />
        <TextField
          label="Image URL"
          hint="Optional. A banner across the bottom or a small thumbnail."
          value={values.imageUrl ?? ""}
          onChange={(v) => patch({ imageUrl: v || null })}
          placeholder="https://…"
        />
        <SelectField
          label="Image style"
          value={values.imageType}
          onChange={(v) => patch({ imageType: v })}
          options={[
            { value: "banner", label: "Banner" },
            { value: "thumbnail", label: "Thumbnail" },
          ]}
        />
        <SelectField
          label="Layout"
          hint="Buttons, a single dropdown, or a mix you choose per topic."
          value={values.displayMode}
          onChange={(v) => patch({ displayMode: v })}
          options={[
            { value: "buttons", label: "Buttons" },
            { value: "dropdown", label: "Dropdown menu" },
            { value: "mixed", label: "Mixed" },
          ]}
        />
      </Section>

      {/* Topics & groups */}
      <section className="glass rounded-xl">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-[var(--border)] p-5 pb-4">
          <div>
            <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">
              Topics &amp; groups
            </h2>
            <p className="text-sm text-fg-muted">
              What members can pick. {totalItems}/{MAX_PANEL_ITEMS} items used.
            </p>
          </div>
          <div className="flex gap-2">
            <Button variant="secondary" size="sm" onClick={addCategory}>
              <Plus aria-hidden />
              Add group
            </Button>
          </div>
        </div>

        <div className="space-y-3 p-5">
          {availableTopics.length > 0 && (
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm text-fg-muted">Add a topic:</span>
              <div className="min-w-[14rem] flex-1">
                <EntityPicker
                  items={availableTopics.map((t) => ({
                    value: t.name,
                    label: t.label,
                    description: t.name,
                  }))}
                  value={null}
                  onChange={(v) => v && addTopic(v)}
                  placeholder="Choose a topic to add…"
                  searchPlaceholder="Search topics…"
                  emptyMessage="All topics are on this panel."
                  aria-label="Add a topic"
                />
              </div>
            </div>
          )}

          {orderedEntries.length === 0 && (
            <p className="rounded-lg border border-dashed border-[var(--border)] p-6 text-center text-sm text-fg-muted">
              No topics on this panel yet. Add one above, or make a group.
            </p>
          )}

          {orderedEntries.map((entry, index) =>
            entry.kind === "topic" ? (
              <TopicRow
                key={`t:${entry.name}`}
                topic={topicsByName.get(entry.name)}
                name={entry.name}
                display={values.displayMode}
                displayAs={values.topicDisplayMap[entry.name] ?? "button"}
                onDisplayChange={(d) =>
                  patch({ topicDisplayMap: { ...values.topicDisplayMap, [entry.name]: d } })
                }
                onUp={() => move(index, -1)}
                onDown={() => move(index, 1)}
                onRemove={() => removeTopic(entry.name)}
              />
            ) : (
              <CategoryRow
                key={`c:${entry.cat.slug}`}
                cat={entry.cat}
                topicsByName={topicsByName}
                attachedTopics={values.topicNames}
                categorisedElsewhere={new Set(
                  values.categories
                    .filter((c) => c.slug !== entry.cat.slug)
                    .flatMap((c) => c.topicNames),
                )}
                standalone={orderedEntries
                  .filter((e) => e.kind === "topic")
                  .map((e) => (e as { name: string }).name)}
                onChange={(next) => updateCategory(entry.cat.slug, next)}
                onSetTopics={(names) => setCategoryTopics(entry.cat.slug, names)}
                onUp={() => move(index, -1)}
                onDown={() => move(index, 1)}
                onRemove={() => removeCategory(entry.cat.slug)}
              />
            ),
          )}
        </div>
      </section>

      <Preview panel={values} topicsByName={topicsByName} />

      <section className="glass rounded-xl border-[var(--danger)]/25 p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-sm font-semibold text-[var(--danger)]">Delete this panel</h2>
            <p className="text-sm text-fg-muted">Removes the config and takes down its live message.</p>
          </div>
          <Button variant="danger" onClick={remove}>
            <Trash2 aria-hidden />
            Delete panel
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

function RowShell({
  children,
  onUp,
  onDown,
  onRemove,
}: {
  children: React.ReactNode;
  onUp: () => void;
  onDown: () => void;
  onRemove: () => void;
}) {
  return (
    <div className="flex items-start gap-2 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3">
      <div className="flex flex-col gap-1 pt-0.5">
        <button type="button" onClick={onUp} className="text-fg-subtle hover:text-fg" aria-label="Move up">
          <ArrowUp className="size-4" aria-hidden />
        </button>
        <button type="button" onClick={onDown} className="text-fg-subtle hover:text-fg" aria-label="Move down">
          <ArrowDown className="size-4" aria-hidden />
        </button>
      </div>
      <div className="min-w-0 flex-1">{children}</div>
      <Button variant="ghost" size="icon-sm" onClick={onRemove} aria-label="Remove">
        <X aria-hidden />
      </Button>
    </div>
  );
}

function TopicRow({
  topic,
  name,
  display,
  displayAs,
  onDisplayChange,
  onUp,
  onDown,
  onRemove,
}: {
  topic: Topic | undefined;
  name: string;
  display: Panel["displayMode"];
  displayAs: "button" | "dropdown";
  onDisplayChange: (d: "button" | "dropdown") => void;
  onUp: () => void;
  onDown: () => void;
  onRemove: () => void;
}) {
  return (
    <RowShell onUp={onUp} onDown={onDown} onRemove={onRemove}>
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-medium">
          {topic?.emoji ? `${topic.emoji} ` : ""}
          {topic?.label ?? name}
        </span>
        {!topic && <Badge variant="danger">missing</Badge>}
        <code className="text-xs text-fg-subtle">{name}</code>
        {display === "mixed" && (
          <div className="ml-auto w-40">
            <EntityPicker
              items={[
                { value: "button", label: "As a button" },
                { value: "dropdown", label: "In the dropdown" },
              ]}
              value={displayAs}
              clearable={false}
              onChange={(v) => v && onDisplayChange(v as "button" | "dropdown")}
              aria-label="How to show this topic"
            />
          </div>
        )}
      </div>
    </RowShell>
  );
}

function CategoryRow({
  cat,
  topicsByName,
  attachedTopics,
  categorisedElsewhere,
  standalone,
  onChange,
  onSetTopics,
  onUp,
  onDown,
  onRemove,
}: {
  cat: PanelCategory;
  topicsByName: Map<string, Topic>;
  attachedTopics: string[];
  categorisedElsewhere: Set<string>;
  standalone: string[];
  onChange: (next: Partial<PanelCategory>) => void;
  onSetTopics: (names: string[]) => void;
  onUp: () => void;
  onDown: () => void;
  onRemove: () => void;
}) {
  // A category can hold any attached topic that isn't in another category. Its
  // own members plus the currently-standalone ones are the choices.
  const choices = attachedTopics.filter(
    (n) => !categorisedElsewhere.has(n) && (cat.topicNames.includes(n) || standalone.includes(n)),
  );
  return (
    <RowShell onUp={onUp} onDown={onDown} onRemove={onRemove}>
      <div className="space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="accent">Group</Badge>
          <Input
            value={cat.label}
            onChange={(e) => onChange({ label: e.target.value })}
            className="h-9 max-w-[16rem]"
            aria-label="Group label"
          />
          <Input
            value={cat.emoji ?? ""}
            onChange={(e) => onChange({ emoji: e.target.value || null })}
            className="h-9 w-20"
            placeholder="Emoji"
            aria-label="Group emoji"
          />
          <div className="w-32">
            <EntityPicker
              items={[
                { value: "primary", label: "Blurple" },
                { value: "secondary", label: "Grey" },
                { value: "success", label: "Green" },
                { value: "danger", label: "Red" },
              ]}
              value={cat.buttonColor}
              clearable={false}
              onChange={(v) => v && onChange({ buttonColor: v as ButtonColor })}
              aria-label="Group button colour"
            />
          </div>
        </div>
        <Field hint="Topics in this group open from a second menu when the group button is clicked.">
          <EntityPicker
            multiple
            items={choices.map((n) => ({
              value: n,
              label: topicsByName.get(n)?.label ?? n,
              description: n,
            }))}
            value={cat.topicNames.filter((n) => attachedTopics.includes(n))}
            onChange={onSetTopics}
            placeholder="Choose topics for this group…"
            searchPlaceholder="Search topics…"
            emptyMessage="Add topics to the panel first."
            aria-label="Group topics"
          />
        </Field>
        {cat.topicNames.length === 0 && (
          <p className="text-xs text-[var(--warning)]">
            An empty group is hidden from the panel until it has a topic.
          </p>
        )}
      </div>
    </RowShell>
  );
}

/** A rough approximation of how the panel reads in Discord. */
function Preview({
  panel,
  topicsByName,
}: {
  panel: Panel;
  topicsByName: Map<string, Topic>;
}) {
  const entries = panel.topicOrder
    .map((entry) => {
      if (entry.startsWith("cat:")) {
        const cat = panel.categories.find((c) => c.slug === entry.slice(4));
        return cat && cat.topicNames.length > 0 ? ({ kind: "category", cat } as const) : null;
      }
      const inCat = panel.categories.some((c) => c.topicNames.includes(entry));
      return panel.topicNames.includes(entry) && !inCat
        ? ({ kind: "topic", name: entry } as const)
        : null;
    })
    .filter((e): e is NonNullable<typeof e> => e !== null);

  const asButton = (name: string) =>
    panel.displayMode === "buttons" ||
    (panel.displayMode === "mixed" && (panel.topicDisplayMap[name] ?? "button") === "button");

  const buttons = entries.filter((e) => e.kind === "category" || asButton(e.name));
  const dropdownTopics = entries.filter((e) => e.kind === "topic" && !asButton(e.name));

  const hasEmbed = Boolean(panel.title || panel.description || panel.imageUrl);

  return (
    <Section title="Preview" description="Roughly how it reads in Discord.">
      <div className="sm:col-span-2 space-y-3">
        {/* The embed: a coloured left bar, title and description, with the image
            as a thumbnail beside them or a banner beneath — the way Discord
            lays one out. */}
        <div className="overflow-hidden rounded-md border border-[var(--border)] border-l-4 border-l-[var(--accent)] bg-[var(--surface)] p-3">
          <div className="flex gap-3">
            <div className="min-w-0 flex-1">
              {panel.title && <p className="font-semibold">{panel.title}</p>}
              {panel.description && (
                <p className="mt-1 whitespace-pre-wrap text-sm text-fg-muted">{panel.description}</p>
              )}
              {!hasEmbed && <p className="text-sm text-fg-subtle">No title, description or image yet.</p>}
            </div>
            {panel.imageUrl && panel.imageType === "thumbnail" && (
              <PreviewImage url={panel.imageUrl} variant="thumbnail" />
            )}
          </div>
          {panel.imageUrl && panel.imageType === "banner" && (
            <PreviewImage url={panel.imageUrl} variant="banner" />
          )}
        </div>

        {/* Components sit below the embed, as they do in Discord. */}
        <div className="flex flex-wrap gap-2">
          {buttons.map((e) =>
            e.kind === "category" ? (
              <span
                key={`c:${e.cat.slug}`}
                className={cn("rounded-md px-3 py-1.5 text-sm font-medium", COLOR_CLASS[e.cat.buttonColor])}
              >
                {e.cat.emoji ? `${e.cat.emoji} ` : ""}
                {e.cat.label}
              </span>
            ) : (
              <span
                key={`t:${e.name}`}
                className={cn(
                  "rounded-md px-3 py-1.5 text-sm font-medium",
                  COLOR_CLASS[topicsByName.get(e.name)?.buttonColor ?? "secondary"],
                )}
              >
                {topicsByName.get(e.name)?.emoji ? `${topicsByName.get(e.name)!.emoji} ` : ""}
                {topicsByName.get(e.name)?.label ?? e.name}
              </span>
            ),
          )}
        </div>
        {dropdownTopics.length > 0 && (
          <div className="rounded-md border border-[var(--border-strong)] px-3 py-2 text-sm text-fg-muted">
            ▾ {dropdownTopics.length} option{dropdownTopics.length === 1 ? "" : "s"} in a dropdown
          </div>
        )}
        {buttons.length === 0 && dropdownTopics.length === 0 && (
          <p className="text-sm text-fg-subtle">No topics added yet.</p>
        )}
      </div>
    </Section>
  );
}

/** A panel image in the preview, hidden if the URL doesn't load. */
function PreviewImage({ url, variant }: { url: string; variant: "banner" | "thumbnail" }) {
  const [ok, setOk] = React.useState(true);
  React.useEffect(() => setOk(true), [url]);
  if (!ok) return null;
  return (
    // eslint-disable-next-line @next/next/no-img-element
    <img
      src={url}
      alt=""
      onError={() => setOk(false)}
      className={cn(
        "bg-[var(--surface-hover)] object-cover",
        variant === "thumbnail"
          ? "size-20 shrink-0 rounded-md"
          : "mt-3 max-h-72 w-full rounded-md",
      )}
    />
  );
}
