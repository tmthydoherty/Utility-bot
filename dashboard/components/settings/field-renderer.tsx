"use client";

import * as React from "react";
import { Minus, Plus } from "lucide-react";

import { cn, formatDuration } from "@/lib/utils";
import { FieldType, type Field, type FieldValue } from "@/lib/schema/types";
import { useChannelItems, useRoleItems } from "@/components/providers/guild-provider";
import { EntityPicker, type PickerItem } from "@/components/ui/entity-picker";
import { FieldError, FieldHint, Input, Label, Textarea } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";

/**
 * One `FieldType` -> one control. This switch is the entire reason a new module
 * is a schema file rather than a page.
 *
 * Every branch takes the same props and reports back through the same
 * `onChange`, so the form above never needs to know what kind of field it is
 * holding — which is what makes conditional visibility, dirty tracking and
 * validation generic.
 */

interface FieldControlProps {
  field: Field;
  value: FieldValue;
  onChange: (value: FieldValue) => void;
  error?: string;
  disabled?: boolean;
}

export function FieldRow({ field, value, onChange, error, disabled }: FieldControlProps) {
  const id = `field-${field.key}`;
  const describedBy = [field.help && `${id}-hint`, error && `${id}-error`]
    .filter(Boolean)
    .join(" ");

  // A boolean reads better as a row with the switch on the right than as a
  // label stacked above a control — it is a statement you turn on, not a value
  // you enter.
  if (field.type === FieldType.BOOL) {
    return (
      <div className={cn("flex items-start justify-between gap-4 py-1", field.wide && "sm:col-span-2")}>
        <div className="min-w-0 space-y-1">
          <Label htmlFor={id}>{field.label}</Label>
          {field.help && <FieldHint id={`${id}-hint`}>{field.help}</FieldHint>}
        </div>
        <Switch
          id={id}
          checked={value === true}
          onCheckedChange={(checked) => onChange(checked)}
          disabled={disabled}
          aria-describedby={describedBy || undefined}
        />
      </div>
    );
  }

  return (
    <div className={cn("space-y-2", field.wide && "sm:col-span-2")}>
      <Label htmlFor={id} required={field.required}>
        {field.label}
      </Label>
      <FieldControl
        field={field}
        value={value}
        onChange={onChange}
        error={error}
        disabled={disabled}
      />
      {field.help && <FieldHint id={`${id}-hint`}>{field.help}</FieldHint>}
      <FieldError id={`${id}-error`}>{error}</FieldError>
    </div>
  );
}

function FieldControl({ field, value, onChange, error, disabled }: FieldControlProps) {
  const id = `field-${field.key}`;
  const invalid = Boolean(error);

  switch (field.type) {
    case FieldType.TEXT:
      return (
        <Input
          id={id}
          value={typeof value === "string" ? value : ""}
          onChange={(event) => onChange(event.target.value)}
          placeholder={field.placeholder}
          maxLength={field.max}
          disabled={disabled}
          aria-invalid={invalid}
        />
      );

    case FieldType.MULTILINE:
      return (
        <MultilineField field={field} value={value} onChange={onChange} disabled={disabled} invalid={invalid} />
      );

    case FieldType.NUMBER:
      return (
        <NumberField field={field} value={value} onChange={onChange} disabled={disabled} invalid={invalid} />
      );

    case FieldType.DURATION:
      return (
        <DurationField field={field} value={value} onChange={onChange} disabled={disabled} invalid={invalid} />
      );

    case FieldType.CHANNEL:
      return (
        <ChannelField field={field} value={value} onChange={onChange} disabled={disabled} invalid={invalid} />
      );

    case FieldType.ROLE:
      return <RoleField field={field} value={value} onChange={onChange} disabled={disabled} invalid={invalid} />;

    case FieldType.USER:
      // No member list is loaded — fetching every member of a large guild to
      // populate a dropdown is not a thing worth doing — so this takes IDs
      // directly, the same as the Discord panel does.
      return (
        <IdListField field={field} value={value} onChange={onChange} disabled={disabled} invalid={invalid} />
      );

    case FieldType.EMOJI:
      return (
        <Input
          id={id}
          value={typeof value === "string" ? value : ""}
          onChange={(event) => onChange(event.target.value)}
          placeholder={field.placeholder ?? "😀 or :custom_name:"}
          disabled={disabled}
          aria-invalid={invalid}
        />
      );

    case FieldType.CHOICE:
      return (
        <EntityPicker
          id={id}
          items={(field.choices ?? []).map<PickerItem>((choice) => ({
            value: choice.value,
            label: choice.label,
            description: choice.description,
          }))}
          value={typeof value === "string" ? value : null}
          onChange={onChange}
          placeholder={field.placeholder ?? "Select an option"}
          disabled={disabled}
          invalid={invalid}
        />
      );

    default:
      return null;
  }
}

interface ControlProps {
  field: Field;
  value: FieldValue;
  onChange: (value: FieldValue) => void;
  disabled?: boolean;
  invalid?: boolean;
}

function MultilineField({ field, value, onChange, disabled, invalid }: ControlProps) {
  const text = typeof value === "string" ? value : "";
  const max = field.max ?? 2000;
  // Only warn near the ceiling. A counter under every textarea is noise; a
  // counter that appears when it starts to matter is information.
  const showCount = text.length > max * 0.8;

  return (
    <div className="space-y-1.5">
      <Textarea
        id={`field-${field.key}`}
        value={text}
        onChange={(event) => onChange(event.target.value)}
        placeholder={field.placeholder}
        maxLength={max}
        disabled={disabled}
        aria-invalid={invalid}
      />
      {showCount && (
        <p
          className={cn(
            "text-right text-xs [font-variant-numeric:tabular-nums]",
            text.length >= max ? "text-[var(--danger)]" : "text-fg-subtle",
          )}
        >
          {text.length} / {max}
        </p>
      )}
    </div>
  );
}

function NumberField({ field, value, onChange, disabled, invalid }: ControlProps) {
  const current = typeof value === "number" ? value : (field.min ?? 0);

  const nudge = (delta: number) => {
    const next = current + delta;
    if (field.min !== undefined && next < field.min) return;
    if (field.max !== undefined && next > field.max) return;
    onChange(next);
  };

  return (
    <div className="flex items-center gap-2">
      {/* Steppers exist for touch: a number input's native spinners are a few
          pixels tall and effectively unusable with a finger. */}
      <Button
        type="button"
        variant="secondary"
        size="icon"
        onClick={() => nudge(-1)}
        disabled={disabled || (field.min !== undefined && current <= field.min)}
        aria-label={`Decrease ${field.label}`}
      >
        <Minus aria-hidden />
      </Button>
      <Input
        id={`field-${field.key}`}
        type="number"
        inputMode="numeric"
        className="text-center [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
        value={current}
        min={field.min}
        max={field.max}
        onChange={(event) => {
          const parsed = Number(event.target.value);
          onChange(Number.isFinite(parsed) ? parsed : 0);
        }}
        disabled={disabled}
        aria-invalid={invalid}
      />
      <Button
        type="button"
        variant="secondary"
        size="icon"
        onClick={() => nudge(1)}
        disabled={disabled || (field.max !== undefined && current >= field.max)}
        aria-label={`Increase ${field.label}`}
      >
        <Plus aria-hidden />
      </Button>
    </div>
  );
}

const DURATION_UNITS = [
  { label: "seconds", seconds: 1 },
  { label: "minutes", seconds: 60 },
  { label: "hours", seconds: 3600 },
  { label: "days", seconds: 86400 },
] as const;

function DurationField({ field, value, onChange, disabled, invalid }: ControlProps) {
  const totalSeconds = typeof value === "number" ? value : 0;

  // Show the value in the largest unit it divides into cleanly, so 3600 reads
  // as "1 hour" rather than "3600 seconds".
  const unit = React.useMemo(() => {
    for (let i = DURATION_UNITS.length - 1; i >= 0; i--) {
      const candidate = DURATION_UNITS[i]!;
      if (totalSeconds > 0 && totalSeconds % candidate.seconds === 0) return candidate;
    }
    return DURATION_UNITS[0]!;
  }, [totalSeconds]);

  const amount = totalSeconds / unit.seconds;

  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-2">
        <Input
          id={`field-${field.key}`}
          type="number"
          inputMode="numeric"
          className="flex-1"
          value={amount}
          min={0}
          onChange={(event) => {
            const parsed = Number(event.target.value);
            onChange(Number.isFinite(parsed) ? Math.max(0, parsed) * unit.seconds : 0);
          }}
          disabled={disabled}
          aria-invalid={invalid}
        />
        <EntityPicker
          items={DURATION_UNITS.map((u) => ({ value: u.label, label: u.label }))}
          value={unit.label}
          onChange={(next) => {
            const chosen = DURATION_UNITS.find((u) => u.label === next);
            if (chosen) onChange(amount * chosen.seconds);
          }}
          disabled={disabled}
          // There is no "no unit" — clearing it would leave the number
          // meaning nothing.
          clearable={false}
          aria-label={`Unit for ${field.label}`}
          className="w-36 shrink-0"
        />
      </div>
      <p className="text-xs text-fg-subtle">
        {totalSeconds === 0 ? "Disabled" : formatDuration(totalSeconds)}
      </p>
    </div>
  );
}

function ChannelField({ field, value, onChange, disabled, invalid }: ControlProps) {
  const items = useChannelItems(field.channelTypes);
  return field.multi ? (
    <EntityPicker
      id={`field-${field.key}`}
      multiple
      items={items}
      value={Array.isArray(value) ? value : []}
      onChange={onChange}
      placeholder="Select channels"
      searchPlaceholder="Search channels…"
      emptyMessage="No matching channels."
      disabled={disabled}
      invalid={invalid}
    />
  ) : (
    <EntityPicker
      id={`field-${field.key}`}
      items={items}
      value={typeof value === "string" ? value : null}
      onChange={onChange}
      placeholder="Select a channel"
      searchPlaceholder="Search channels…"
      emptyMessage="No matching channels."
      disabled={disabled}
      invalid={invalid}
    />
  );
}

function RoleField({ field, value, onChange, disabled, invalid }: ControlProps) {
  const items = useRoleItems();
  return field.multi ? (
    <EntityPicker
      id={`field-${field.key}`}
      multiple
      items={items}
      value={Array.isArray(value) ? value : []}
      onChange={onChange}
      placeholder="Select roles"
      searchPlaceholder="Search roles…"
      emptyMessage="No matching roles."
      disabled={disabled}
      invalid={invalid}
    />
  ) : (
    <EntityPicker
      id={`field-${field.key}`}
      items={items}
      value={typeof value === "string" ? value : null}
      onChange={onChange}
      placeholder="Select a role"
      searchPlaceholder="Search roles…"
      emptyMessage="No matching roles."
      disabled={disabled}
      invalid={invalid}
    />
  );
}

/** Free-entry list of Discord IDs, for entities with no fetchable list. */
function IdListField({ field, value, onChange, disabled, invalid }: ControlProps) {
  const [draft, setDraft] = React.useState("");
  const ids = Array.isArray(value) ? value : typeof value === "string" && value ? [value] : [];

  const add = () => {
    const trimmed = draft.trim();
    if (!/^\d{17,20}$/.test(trimmed) || ids.includes(trimmed)) return;
    onChange(field.multi ? [...ids, trimmed] : trimmed);
    setDraft("");
  };

  return (
    <div className="space-y-2">
      <div className="flex gap-2">
        <Input
          id={`field-${field.key}`}
          value={draft}
          inputMode="numeric"
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              add();
            }
          }}
          placeholder={field.placeholder ?? "Paste a user ID"}
          disabled={disabled}
          aria-invalid={invalid}
        />
        <Button type="button" variant="secondary" onClick={add} disabled={disabled || !draft.trim()}>
          Add
        </Button>
      </div>
      {ids.length > 0 && (
        <ul className="flex flex-wrap gap-1.5">
          {ids.map((id) => (
            <li key={id}>
              <button
                type="button"
                onClick={() => onChange(field.multi ? ids.filter((v) => v !== id) : null)}
                disabled={disabled}
                className={cn(
                  "inline-flex items-center gap-1.5 rounded-full bg-[var(--surface-hover)]",
                  "px-2.5 py-1 font-mono text-xs transition-colors hover:bg-[var(--surface-active)]",
                )}
                aria-label={`Remove ${id}`}
              >
                {id}
                <Minus className="size-3" aria-hidden />
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
