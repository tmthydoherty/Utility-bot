"use client";

import { Plus, Trash2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EntityPicker } from "@/components/ui/entity-picker";
import { Input, Textarea, Label, FieldHint } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { useRoleItems } from "@/components/providers/guild-provider";
import type { Embed, LinkButton, RoleButton } from "@/lib/utility/types";

import { EmbedBuilder } from "./embed-builder";

export interface MessageContent {
  plainText: boolean;
  content: string;
  pingRoleId: string | null;
  embed: Embed;
  buttons: LinkButton[];
  reactionRole: RoleButton | null;
}

const BUTTON_STYLES: { value: RoleButton["style"]; label: string }[] = [
  { value: "primary", label: "Blurple" },
  { value: "secondary", label: "Grey" },
  { value: "success", label: "Green" },
  { value: "danger", label: "Red" },
];

/**
 * Everything about *what the message says* — shared by reminders and stickies.
 * The schedule and where it posts live in the editor around this; this is the
 * body, the ping, the embed and the buttons.
 */
export function MessageComposer({
  value,
  onChange,
}: {
  value: MessageContent;
  onChange: (value: MessageContent) => void;
}) {
  const roleItems = useRoleItems();
  const set = <K extends keyof MessageContent>(key: K, val: MessageContent[K]) =>
    onChange({ ...value, [key]: val });

  const addButton = () => {
    if (value.buttons.length >= 5) return;
    set("buttons", [...value.buttons, { type: "link", label: "", url: "" }]);
  };
  const setButton = (index: number, patch: Partial<LinkButton>) =>
    set(
      "buttons",
      value.buttons.map((b, i) => (i === index ? { ...b, ...patch } : b)),
    );
  const removeButton = (index: number) =>
    set("buttons", value.buttons.filter((_, i) => i !== index));

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Label htmlFor="msg-content">Text above the message</Label>
        <Textarea
          id="msg-content"
          value={value.content}
          maxLength={2000}
          rows={2}
          onChange={(e) => set("content", e.target.value)}
          placeholder="Optional. This is the only place a role ping actually notifies people."
        />
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        <div className="space-y-2">
          <Label>Ping a role</Label>
          <EntityPicker
            items={roleItems}
            value={value.pingRoleId}
            onChange={(next) => set("pingRoleId", next)}
            placeholder="No ping"
            aria-label="Role to ping"
          />
          <FieldHint>Added to the top of the text above, so it notifies.</FieldHint>
        </div>
        <div className="space-y-2">
          <Label htmlFor="msg-plain">Send as plain text</Label>
          <label className="flex h-9 items-center gap-2 text-sm text-fg-muted">
            <Switch
              id="msg-plain"
              checked={value.plainText}
              onCheckedChange={(checked) => set("plainText", checked)}
            />
            {value.plainText ? "No embed — just text" : "Rich embed"}
          </label>
        </div>
      </div>

      {!value.plainText && <EmbedBuilder value={value.embed} onChange={(embed) => set("embed", embed)} />}

      {/* Role button */}
      <Card className="space-y-4 p-4">
        <div className="flex items-center justify-between">
          <div>
            <Label>Role button</Label>
            <FieldHint>A button under the message that gives or removes a role when clicked.</FieldHint>
          </div>
          <Switch
            checked={value.reactionRole !== null}
            onCheckedChange={(checked) =>
              set(
                "reactionRole",
                checked ? { roleId: "", label: "Get Role", style: "secondary" } : null,
              )
            }
            aria-label="Enable role button"
          />
        </div>
        {value.reactionRole !== null && (
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label>Role</Label>
              <EntityPicker
                items={roleItems}
                value={value.reactionRole.roleId || null}
                onChange={(next) =>
                  set("reactionRole", { ...value.reactionRole!, roleId: next ?? "" })
                }
                placeholder="Pick a role"
                aria-label="Button role"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="rr-label">Button label</Label>
              <Input
                id="rr-label"
                value={value.reactionRole.label}
                maxLength={80}
                onChange={(e) => set("reactionRole", { ...value.reactionRole!, label: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="rr-emoji">Emoji (optional)</Label>
              <Input
                id="rr-emoji"
                value={value.reactionRole.emoji ?? ""}
                onChange={(e) => set("reactionRole", { ...value.reactionRole!, emoji: e.target.value })}
                placeholder="🔔 or <:name:id>"
              />
            </div>
            <div className="space-y-2">
              <Label>Colour</Label>
              <EntityPicker
                items={BUTTON_STYLES.map((s) => ({ value: s.value, label: s.label }))}
                value={value.reactionRole.style}
                onChange={(next) =>
                  set("reactionRole", {
                    ...value.reactionRole!,
                    style: (next as RoleButton["style"]) ?? "secondary",
                  })
                }
                clearable={false}
                aria-label="Button colour"
              />
            </div>
          </div>
        )}
      </Card>

      {/* Link buttons */}
      <Card className="space-y-4 p-4">
        <div className="flex items-center justify-between">
          <div>
            <Label>Link buttons</Label>
            <FieldHint>Buttons that open a URL. Up to five.</FieldHint>
          </div>
          <Button type="button" variant="ghost" size="sm" onClick={addButton} disabled={value.buttons.length >= 5}>
            <Plus aria-hidden /> Add
          </Button>
        </div>
        {value.buttons.map((btn, index) => (
          <div key={index} className="grid gap-2 sm:grid-cols-[1fr_1.4fr_auto]">
            <Input
              value={btn.label}
              maxLength={80}
              placeholder="Label"
              onChange={(e) => setButton(index, { label: e.target.value })}
            />
            <Input
              value={btn.url}
              placeholder="https://…"
              onChange={(e) => setButton(index, { url: e.target.value })}
            />
            <Button
              type="button"
              variant="ghost"
              size="icon-sm"
              onClick={() => removeButton(index)}
              aria-label="Remove button"
            >
              <Trash2 aria-hidden />
            </Button>
          </div>
        ))}
      </Card>
    </div>
  );
}
