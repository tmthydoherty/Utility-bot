"use client";

import * as React from "react";
import { GripVertical, Plus, X } from "lucide-react";

import { cn } from "@/lib/utils";
import { ChannelType } from "@/lib/discord/types";
import { Button } from "@/components/ui/button";
import { Input, Textarea, Label, FieldHint } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { EntityPicker, type PickerItem } from "@/components/ui/entity-picker";
import { useChannelItems, useRoleItems, useGuild } from "@/components/providers/guild-provider";

/**
 * The form controls the topic editor and panel builder are built from.
 *
 * Kept together so the two editors read as configuration, not markup, and so a
 * change to how (say) a channel picker looks lands in one place. Each control is
 * a thin, labelled wrapper over the shared primitives — the same EntityPicker,
 * Switch and Input the schema-driven settings form uses.
 */

export function Section({
  title,
  description,
  children,
}: {
  title: string;
  description?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="glass rounded-xl">
      <div className="space-y-1 border-b border-[var(--border)] p-5 pb-4">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">{title}</h2>
        {description && <p className="text-sm text-fg-muted">{description}</p>}
      </div>
      <div className="grid gap-5 p-5 sm:grid-cols-2">{children}</div>
    </section>
  );
}

export function Field({
  label,
  hint,
  htmlFor,
  wide,
  children,
}: {
  label?: string;
  hint?: string;
  htmlFor?: string;
  wide?: boolean;
  children: React.ReactNode;
}) {
  return (
    <div className={cn("space-y-2", wide && "sm:col-span-2")}>
      {label && <Label htmlFor={htmlFor}>{label}</Label>}
      {children}
      {hint && <FieldHint>{hint}</FieldHint>}
    </div>
  );
}

export function TextField({
  label,
  hint,
  value,
  onChange,
  placeholder,
  maxLength,
  wide,
  id,
}: {
  label?: string;
  hint?: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  maxLength?: number;
  wide?: boolean;
  id?: string;
}) {
  return (
    <Field label={label} hint={hint} htmlFor={id} wide={wide}>
      <Input
        id={id}
        value={value}
        placeholder={placeholder}
        maxLength={maxLength}
        onChange={(e) => onChange(e.target.value)}
      />
    </Field>
  );
}

export function TextAreaField({
  label,
  hint,
  value,
  onChange,
  placeholder,
  rows = 3,
  wide = true,
}: {
  label?: string;
  hint?: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  rows?: number;
  wide?: boolean;
}) {
  return (
    <Field label={label} hint={hint} wide={wide}>
      <Textarea
        value={value}
        rows={rows}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
      />
    </Field>
  );
}

export function NumberField({
  label,
  hint,
  value,
  onChange,
  min = 0,
  max,
}: {
  label?: string;
  hint?: string;
  value: number;
  onChange: (v: number) => void;
  min?: number;
  max?: number;
}) {
  return (
    <Field label={label} hint={hint}>
      <Input
        type="number"
        value={String(value)}
        min={min}
        max={max}
        onChange={(e) => {
          const n = Number(e.target.value);
          onChange(Number.isFinite(n) ? n : min);
        }}
      />
    </Field>
  );
}

/** A labelled on/off row — the label and switch on one line, hint beneath. */
export function Toggle({
  label,
  hint,
  checked,
  onChange,
  wide = true,
}: {
  label: string;
  hint?: string;
  checked: boolean;
  onChange: (v: boolean) => void;
  wide?: boolean;
}) {
  return (
    <div className={cn("space-y-1.5", wide && "sm:col-span-2")}>
      <div className="flex items-center justify-between gap-4">
        <Label className="cursor-pointer">{label}</Label>
        <Switch checked={checked} onCheckedChange={onChange} aria-label={label} />
      </div>
      {hint && <FieldHint>{hint}</FieldHint>}
    </div>
  );
}

export function SelectField<T extends string>({
  label,
  hint,
  value,
  onChange,
  options,
  clearable = false,
  wide,
}: {
  label?: string;
  hint?: string;
  value: T;
  onChange: (v: T) => void;
  options: { value: T; label: string; description?: string }[];
  clearable?: boolean;
  wide?: boolean;
}) {
  return (
    <Field label={label} hint={hint} wide={wide}>
      <EntityPicker
        items={options as PickerItem[]}
        value={value}
        clearable={clearable}
        onChange={(v) => v && onChange(v as T)}
        aria-label={label}
      />
    </Field>
  );
}

export function ChannelField({
  label,
  hint,
  value,
  onChange,
  types,
  wide,
}: {
  label?: string;
  hint?: string;
  value: string | null;
  onChange: (v: string | null) => void;
  types?: number[];
  wide?: boolean;
}) {
  const items = useChannelItems(types);
  return (
    <Field label={label} hint={hint} wide={wide}>
      <EntityPicker
        items={items}
        value={value}
        onChange={onChange}
        placeholder="Select a channel"
        searchPlaceholder="Search channels…"
        emptyMessage="No matching channels."
        aria-label={label}
      />
    </Field>
  );
}

/** Categories only — a topic in channel mode opens tickets under one. */
export function CategoryField({
  label,
  hint,
  value,
  onChange,
  wide,
}: {
  label?: string;
  hint?: string;
  value: string | null;
  onChange: (v: string | null) => void;
  wide?: boolean;
}) {
  const { channels } = useGuild();
  const items: PickerItem[] = React.useMemo(
    () =>
      channels
        .filter((c) => c.type === ChannelType.GuildCategory)
        .sort((a, b) => a.position - b.position)
        .map((c) => ({ value: c.id, label: c.name, glyph: "📁" })),
    [channels],
  );
  return (
    <Field label={label} hint={hint} wide={wide}>
      <EntityPicker
        items={items}
        value={value}
        onChange={onChange}
        placeholder="Select a category"
        searchPlaceholder="Search categories…"
        emptyMessage="No categories found."
        aria-label={label}
      />
    </Field>
  );
}

export function RoleField({
  label,
  hint,
  value,
  onChange,
  wide,
}: {
  label?: string;
  hint?: string;
  value: string | null;
  onChange: (v: string | null) => void;
  wide?: boolean;
}) {
  const items = useRoleItems();
  return (
    <Field label={label} hint={hint} wide={wide}>
      <EntityPicker
        items={items}
        value={value}
        onChange={onChange}
        placeholder="Select a role"
        searchPlaceholder="Search roles…"
        emptyMessage="No matching roles."
        aria-label={label}
      />
    </Field>
  );
}

export function RolesField({
  label,
  hint,
  value,
  onChange,
  wide = true,
}: {
  label?: string;
  hint?: string;
  value: string[];
  onChange: (v: string[]) => void;
  wide?: boolean;
}) {
  const items = useRoleItems();
  return (
    <Field label={label} hint={hint} wide={wide}>
      <EntityPicker
        multiple
        items={items}
        value={value}
        onChange={onChange}
        placeholder="Select roles"
        searchPlaceholder="Search roles…"
        emptyMessage="No matching roles."
        aria-label={label}
      />
    </Field>
  );
}

/**
 * The survey/application questions — an ordered list of prompts, each up to the
 * 45-character Discord modal label. Add, edit, remove and reorder; the order is
 * the order they're asked in.
 */
export function QuestionsEditor({
  value,
  onChange,
}: {
  value: string[];
  onChange: (v: string[]) => void;
}) {
  const set = (i: number, text: string) =>
    onChange(value.map((q, idx) => (idx === i ? text : q)));
  const remove = (i: number) => onChange(value.filter((_, idx) => idx !== i));
  const move = (i: number, delta: number) => {
    const j = i + delta;
    if (j < 0 || j >= value.length) return;
    const next = [...value];
    [next[i], next[j]] = [next[j]!, next[i]!];
    onChange(next);
  };

  return (
    <div className="space-y-3 sm:col-span-2">
      {value.length === 0 && (
        <p className="text-sm text-fg-muted">
          No questions yet. Add the prompts people answer, in the order you want them asked.
        </p>
      )}
      {value.map((q, i) => (
        <div key={i} className="flex items-start gap-2">
          <div className="flex flex-col pt-1.5">
            <button
              type="button"
              onClick={() => move(i, -1)}
              disabled={i === 0}
              className="text-fg-subtle hover:text-fg disabled:opacity-30"
              aria-label="Move question up"
            >
              <GripVertical className="size-4" aria-hidden />
            </button>
          </div>
          <div className="min-w-0 flex-1">
            <Textarea
              value={q}
              rows={2}
              onChange={(e) => set(i, e.target.value)}
              placeholder={`Question ${i + 1}`}
            />
          </div>
          <Button
            variant="ghost"
            size="icon-sm"
            onClick={() => remove(i)}
            aria-label="Remove question"
            className="mt-1"
          >
            <X aria-hidden />
          </Button>
        </div>
      ))}
      {value.length < 25 && (
        <Button variant="secondary" size="sm" onClick={() => onChange([...value, ""])}>
          <Plus aria-hidden />
          Add question
        </Button>
      )}
    </div>
  );
}

/** A set of Discord user ids, one per line — used for the blacklist. */
export function IdListField({
  label,
  hint,
  value,
  onChange,
}: {
  label?: string;
  hint?: string;
  value: string[];
  onChange: (v: string[]) => void;
}) {
  return (
    <Field label={label} hint={hint} wide>
      <Textarea
        rows={3}
        value={value.join("\n")}
        placeholder="One user ID per line"
        onChange={(e) =>
          onChange(
            e.target.value
              .split(/[\s,]+/)
              .map((s) => s.trim())
              .filter((s) => /^\d{15,20}$/.test(s)),
          )
        }
      />
    </Field>
  );
}
