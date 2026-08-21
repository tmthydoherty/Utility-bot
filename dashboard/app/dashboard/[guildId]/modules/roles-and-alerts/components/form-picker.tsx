"use client";

import { useState } from "react";
import { EntityPicker } from "@/components/ui/entity-picker";
import { useRoleItems, useChannelItems } from "@/components/providers/guild-provider";
import { X, Plus } from "lucide-react";

interface FormPickerProps {
  name: string;
  defaultValue?: string[];
  max?: number;
  disabled?: boolean;
}

export function FormRolePicker({ name, defaultValue = [], max, disabled }: FormPickerProps) {
  const items = useRoleItems();
  const [value, setValue] = useState<string[]>(defaultValue);
  
  const byId = new Map(items.map(i => [i.value, i]));

  const addRole = (v: string | null) => {
    if (v && !value.includes(v)) {
      if (max && value.length >= max) return;
      setValue([...value, v]);
    }
  };

  const removeRole = (id: string) => {
    setValue(value.filter(v => v !== id));
  };
  
  return (
    <div className="space-y-3">
      {(!max || value.length < max) && !disabled && (
        <div className="max-w-xs relative">
          <div className="absolute inset-y-0 left-3 flex items-center pointer-events-none z-10 text-fg-muted">
            <Plus className="size-4" />
          </div>
          <EntityPicker
            items={items.filter(i => !value.includes(i.value))}
            value={null}
            onChange={addRole}
            placeholder={value.length === 0 ? "Select a role..." : "Add another role..."}
            className="pl-9"
            disabled={disabled}
          />
        </div>
      )}

      {value.length > 0 && (
        <div className="flex flex-col gap-2">
          {value.map(id => {
            const item = byId.get(id);
            return (
              <div key={id} className="flex items-center justify-between p-2.5 rounded-md border border-[var(--border-subtle)] bg-[var(--surface-hover)] shadow-sm max-w-sm">
                <div className="flex items-center gap-2.5">
                   {item?.color ? (
                     <div className="size-3 rounded-full shrink-0" style={{ backgroundColor: item.color }} />
                   ) : (
                     <div className="size-3 rounded-full shrink-0 bg-[var(--fg-subtle)]" />
                   )}
                   <span className="text-sm font-medium">{item?.label || id}</span>
                </div>
                {!disabled && (
                  <button
                    type="button"
                    onClick={() => removeRole(id)}
                    className="p-1.5 hover:bg-[var(--surface-active)] rounded text-fg-muted hover:text-[var(--danger)] transition-colors"
                    aria-label={`Remove ${item?.label || id}`}
                  >
                    <X className="size-4" />
                  </button>
                )}
              </div>
            );
          })}
        </div>
      )}

      <input type="hidden" name={name} value={value.join(",")} />
    </div>
  );
}

export function FormChannelPicker({ name, defaultValue = [], disabled }: FormPickerProps) {
  const items = useChannelItems();
  const [value, setValue] = useState<string | null>(defaultValue[0] || null);
  
  const byId = new Map(items.map(i => [i.value, i]));

  const addChannel = (v: string | null) => {
    if (v) setValue(v);
  };

  const removeChannel = () => {
    setValue(null);
  };
  
  const selectedItem = value ? byId.get(value) : null;

  return (
    <div className="space-y-3">
      {!value && !disabled && (
        <div className="max-w-xs relative">
          <div className="absolute inset-y-0 left-3 flex items-center pointer-events-none z-10 text-fg-muted">
            <Plus className="size-4" />
          </div>
          <EntityPicker
            items={items}
            value={null}
            onChange={addChannel}
            placeholder="Select a channel..."
            className="pl-9"
            disabled={disabled}
          />
        </div>
      )}

      {value && (
        <div className="flex flex-col gap-2">
          <div className="flex items-center justify-between p-2.5 rounded-md border border-[var(--border-subtle)] bg-[var(--surface-hover)] shadow-sm max-w-sm">
            <div className="flex items-center gap-2.5">
               <div className="text-fg-muted">
                 {selectedItem?.glyph || "#"}
               </div>
               <div className="flex flex-col">
                 <span className="text-sm font-medium">{selectedItem?.label || value}</span>
                 {selectedItem?.group && (
                   <span className="text-xs text-fg-muted">{selectedItem.group}</span>
                 )}
               </div>
            </div>
            {!disabled && (
              <button
                type="button"
                onClick={removeChannel}
                className="p-1.5 hover:bg-[var(--surface-active)] rounded text-fg-muted hover:text-[var(--danger)] transition-colors"
                aria-label={`Remove ${selectedItem?.label || value}`}
              >
                <X className="size-4" />
              </button>
            )}
          </div>
        </div>
      )}

      <input type="hidden" name={name} value={value || ""} />
    </div>
  );
}
