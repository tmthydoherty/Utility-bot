"use client";

import * as React from "react";
import * as PopoverPrimitive from "@radix-ui/react-popover";
import { Command } from "cmdk";
import { Check, ChevronsUpDown, Search, X } from "lucide-react";

import { cn } from "@/lib/utils";
import { useIsMobile } from "@/lib/hooks/use-media-query";
import { Sheet } from "./sheet";

/**
 * Searchable picker for Discord entities.
 *
 * One component behind ChannelPicker / RolePicker / UserPicker, in single and
 * multi flavours. It presents as a popover on a desktop and as a bottom sheet
 * on a phone — a popover anchored to a control near the bottom of a small
 * screen ends up under the keyboard, which is exactly where the search box is.
 *
 * cmdk supplies the filtering and the roving-focus list semantics; everything
 * visible is ours.
 */

export interface PickerItem {
  value: string;
  label: string;
  /** Optional heading to group under, e.g. a Discord category name. */
  group?: string;
  /** Rendered as a leading dot — role colours. */
  color?: string;
  /** Leading glyph, e.g. # or a speaker for channel types. */
  glyph?: React.ReactNode;
  description?: string;
  disabled?: boolean;
}

interface BaseProps {
  items: PickerItem[];
  placeholder?: string;
  searchPlaceholder?: string;
  emptyMessage?: string;
  disabled?: boolean;
  id?: string;
  invalid?: boolean;
  className?: string;
  /**
   * Whether the selection can be emptied. False where "no value" isn't a
   * meaningful state — a duration's unit, for instance, where clearing it
   * leaves a number with nothing to multiply by.
   */
  clearable?: boolean;
  /** For pickers with no visible <label> of their own. */
  "aria-label"?: string;
}

interface SingleProps extends BaseProps {
  multiple?: false;
  value: string | null;
  onChange: (value: string | null) => void;
}

interface MultiProps extends BaseProps {
  multiple: true;
  value: string[];
  onChange: (value: string[]) => void;
  /** Discord caps most of these at 25; surface the limit rather than truncating. */
  max?: number;
}

export type EntityPickerProps = SingleProps | MultiProps;

export function EntityPicker(props: EntityPickerProps) {
  const {
    items,
    placeholder = "Select…",
    searchPlaceholder = "Search…",
    emptyMessage = "Nothing found.",
    disabled,
    id,
    invalid,
    className,
    clearable = true,
  } = props;

  const [open, setOpen] = React.useState(false);
  const isMobile = useIsMobile();
  // The combobox role requires aria-controls to point at its listbox, and the
  // list is portalled out of the trigger's subtree, so the relationship has to
  // be stated explicitly rather than inferred from nesting.
  const listboxId = React.useId();

  const byValue = React.useMemo(
    () => new Map(items.map((item) => [item.value, item])),
    [items],
  );

  const selected = props.multiple ? props.value : props.value ? [props.value] : [];

  const toggle = (value: string) => {
    if (props.multiple) {
      const next = props.value.includes(value)
        ? props.value.filter((v) => v !== value)
        : [...props.value, value];
      if (props.max && next.length > props.max) return;
      props.onChange(next);
      // Multi stays open — picking five roles should not mean five round trips.
    } else {
      props.onChange(props.value === value ? null : value);
      setOpen(false);
    }
  };

  const clear = (event: React.MouseEvent) => {
    event.stopPropagation();
    if (props.multiple) props.onChange([]);
    else props.onChange(null);
  };

  const groups = React.useMemo(() => {
    const map = new Map<string, PickerItem[]>();
    for (const item of items) {
      const key = item.group ?? "";
      const list = map.get(key);
      if (list) list.push(item);
      else map.set(key, [item]);
    }
    return [...map.entries()];
  }, [items]);

  const list = (
    <Command
      // Values are snowflakes, so cmdk's default substring match on `value`
      // would search IDs rather than names. Match on the label instead.
      filter={(value, search) => {
        const label = byValue.get(value)?.label ?? value;
        return label.toLowerCase().includes(search.toLowerCase()) ? 1 : 0;
      }}
      className="flex max-h-full flex-col overflow-hidden"
    >
      <div className="flex items-center gap-2 border-b border-[var(--border)] px-3">
        <Search className="size-4 shrink-0 text-fg-subtle" aria-hidden />
        <Command.Input
          placeholder={searchPlaceholder}
          className="h-11 w-full bg-transparent text-base outline-none placeholder:text-fg-subtle sm:text-sm"
        />
      </div>
      <Command.List
        id={listboxId}
        className="max-h-[min(60vh,320px)] overflow-y-auto overscroll-contain p-1.5"
      >
        <Command.Empty className="px-3 py-8 text-center text-sm text-fg-muted">
          {emptyMessage}
        </Command.Empty>
        {groups.map(([group, groupItems]) => (
          <Command.Group
            key={group || "ungrouped"}
            heading={group || undefined}
            className={cn(
              "[&_[cmdk-group-heading]]:px-2.5 [&_[cmdk-group-heading]]:pb-1 [&_[cmdk-group-heading]]:pt-3",
              "[&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:font-medium",
              "[&_[cmdk-group-heading]]:uppercase [&_[cmdk-group-heading]]:tracking-wide",
              "[&_[cmdk-group-heading]]:text-fg-subtle",
            )}
          >
            {groupItems.map((item) => {
              const isSelected = selected.includes(item.value);
              return (
                <Command.Item
                  key={item.value}
                  value={item.value}
                  disabled={item.disabled}
                  onSelect={() => toggle(item.value)}
                  className={cn(
                    "flex cursor-pointer items-center gap-2.5 rounded-md px-2.5 py-2.5 text-sm",
                    "data-[selected=true]:bg-[var(--surface-hover)]",
                    "data-[disabled=true]:pointer-events-none data-[disabled=true]:opacity-40",
                  )}
                >
                  {item.color && (
                    <span
                      className="size-2.5 shrink-0 rounded-full"
                      style={{ backgroundColor: item.color }}
                      aria-hidden
                    />
                  )}
                  {item.glyph && <span className="shrink-0 text-fg-subtle">{item.glyph}</span>}
                  <span className="min-w-0 flex-1 truncate">{item.label}</span>
                  {item.description && (
                    <span className="shrink-0 text-xs text-fg-subtle">{item.description}</span>
                  )}
                  {isSelected && <Check className="size-4 shrink-0 text-[var(--accent)]" />}
                </Command.Item>
              );
            })}
          </Command.Group>
        ))}
      </Command.List>
    </Command>
  );

  const trigger = (
    <button
      type="button"
      id={id}
      role="combobox"
      aria-label={props["aria-label"]}
      aria-expanded={open}
      aria-controls={listboxId}
      aria-haspopup="listbox"
      aria-invalid={invalid || undefined}
      disabled={disabled}
      onClick={() => setOpen(true)}
      className={cn(
        "flex min-h-11 w-full items-center gap-2 rounded-md px-3 py-2 text-left",
        "border border-[var(--border-strong)] bg-[var(--surface)]",
        "transition-colors duration-150 hover:border-[var(--fg-subtle)]",
        "focus:border-[var(--accent)] focus:outline-none focus:ring-4 focus:ring-[var(--accent-soft)]",
        "disabled:cursor-not-allowed disabled:opacity-50",
        invalid && "border-[var(--danger)]",
        className,
      )}
    >
      <span className="flex min-w-0 flex-1 flex-wrap items-center gap-1.5">
        {selected.length === 0 && <span className="text-sm text-fg-subtle">{placeholder}</span>}
        {selected.map((value) => {
          const item = byValue.get(value);
          return (
            <span
              key={value}
              className={cn(
                "inline-flex max-w-full items-center gap-1.5 rounded-full",
                "bg-[var(--surface-hover)] py-0.5 pl-2 pr-2 text-xs",
              )}
            >
              {item?.color && (
                <span
                  className="size-2 shrink-0 rounded-full"
                  style={{ backgroundColor: item.color }}
                  aria-hidden
                />
              )}
              <span className="truncate">{item?.label ?? value}</span>
            </span>
          );
        })}
      </span>
      {clearable && selected.length > 0 && (
        // A span, not a button: a nested button is invalid HTML and breaks the
        // combobox's own keyboard handling.
        <span
          role="button"
          tabIndex={-1}
          aria-label="Clear selection"
          onClick={clear}
          className="grid size-6 shrink-0 place-items-center rounded text-fg-subtle transition-colors hover:text-fg"
        >
          <X className="size-3.5" />
        </span>
      )}
      <ChevronsUpDown className="size-4 shrink-0 text-fg-subtle" aria-hidden />
    </button>
  );

  if (isMobile) {
    return (
      <>
        {trigger}
        <Sheet
          open={open}
          onOpenChange={setOpen}
          title={placeholder}
          hideTitle={false}
          className="[&>div:nth-child(3)]:px-0"
        >
          {list}
        </Sheet>
      </>
    );
  }

  return (
    <PopoverPrimitive.Root open={open} onOpenChange={setOpen}>
      <PopoverPrimitive.Trigger asChild>{trigger}</PopoverPrimitive.Trigger>
      <PopoverPrimitive.Portal>
        <PopoverPrimitive.Content
          align="start"
          sideOffset={6}
          className={cn(
            "pop-anim z-50 w-[var(--radix-popover-trigger-width)] min-w-[260px] overflow-hidden rounded-lg",
            "border border-[var(--border-strong)] bg-[var(--surface-solid)] shadow-[var(--elev-4)]",
          )}
        >
          {list}
        </PopoverPrimitive.Content>
      </PopoverPrimitive.Portal>
    </PopoverPrimitive.Root>
  );
}
