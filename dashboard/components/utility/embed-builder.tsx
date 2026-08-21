"use client";

import { Plus, Trash2, GripVertical } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input, Textarea, Label, FieldHint } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import type { Embed, EmbedField } from "@/lib/utility/types";

/**
 * The full Discord embed editor, with a live preview.
 *
 * Every field Discord allows is here — title, description, colour, images,
 * author, footer and up to 25 fields — so an admin can build anything the bot
 * could post, without touching a slash command. The field set and the two
 * gotchas it guards against (pings don't fire inside an embed; a bad image URL
 * rejects the whole message) mirror the bot's `utils/embed_builder.py`.
 */
export function EmbedBuilder({
  value,
  onChange,
}: {
  value: Embed;
  onChange: (embed: Embed) => void;
}) {
  const set = <K extends keyof Embed>(key: K, val: Embed[K]) => onChange({ ...value, [key]: val });

  const setField = (index: number, patch: Partial<EmbedField>) => {
    const fields = value.fields.map((f, i) => (i === index ? { ...f, ...patch } : f));
    onChange({ ...value, fields });
  };
  const addField = () => {
    if (value.fields.length >= 25) return;
    onChange({ ...value, fields: [...value.fields, { name: "", value: "", inline: false }] });
  };
  const removeField = (index: number) => {
    onChange({ ...value, fields: value.fields.filter((_, i) => i !== index) });
  };

  const colorHex = value.color !== null ? `#${value.color.toString(16).padStart(6, "0")}` : "#57f287";

  return (
    <div className="grid gap-6 lg:grid-cols-[1fr_20rem]">
      <div className="space-y-5">
        <div className="space-y-2">
          <Label htmlFor="embed-title">Title</Label>
          <Input
            id="embed-title"
            value={value.title}
            maxLength={256}
            onChange={(e) => set("title", e.target.value)}
            placeholder="Optional heading"
          />
        </div>

        <div className="space-y-2">
          <Label htmlFor="embed-desc">Description</Label>
          <Textarea
            id="embed-desc"
            value={value.description}
            maxLength={4000}
            rows={5}
            onChange={(e) => set("description", e.target.value)}
            placeholder="The main body of the message. Markdown and channel/role mentions work here (but mentions won't ping from inside an embed)."
          />
        </div>

        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="embed-color">Colour</Label>
            <div className="flex items-center gap-3">
              <input
                id="embed-color"
                type="color"
                value={colorHex}
                onChange={(e) => set("color", parseInt(e.target.value.slice(1), 16))}
                className="h-9 w-14 cursor-pointer rounded-md border border-[var(--border)] bg-transparent"
                aria-label="Embed colour"
              />
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => set("color", null)}
                disabled={value.color === null}
              >
                Clear
              </Button>
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="embed-url">Title link</Label>
            <Input
              id="embed-url"
              value={value.url}
              onChange={(e) => set("url", e.target.value)}
              placeholder="https://… (makes the title clickable)"
            />
          </div>
        </div>

        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="embed-image">Image URL</Label>
            <Input
              id="embed-image"
              value={value.imageUrl}
              onChange={(e) => set("imageUrl", e.target.value)}
              placeholder="Large image below the text"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="embed-thumb">Thumbnail URL</Label>
            <Input
              id="embed-thumb"
              value={value.thumbnailUrl}
              onChange={(e) => set("thumbnailUrl", e.target.value)}
              placeholder="Small image, top-right"
            />
          </div>
        </div>

        <details className="rounded-lg border border-[var(--border)] p-4">
          <summary className="cursor-pointer text-sm font-medium">Author &amp; footer</summary>
          <div className="mt-4 space-y-4">
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="embed-author">Author name</Label>
                <Input
                  id="embed-author"
                  value={value.author.name}
                  maxLength={256}
                  onChange={(e) => set("author", { ...value.author, name: e.target.value })}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="embed-author-icon">Author icon URL</Label>
                <Input
                  id="embed-author-icon"
                  value={value.author.iconUrl ?? ""}
                  onChange={(e) => set("author", { ...value.author, iconUrl: e.target.value })}
                />
              </div>
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="embed-footer">Footer text</Label>
                <Input
                  id="embed-footer"
                  value={value.footer.text}
                  maxLength={2048}
                  onChange={(e) => set("footer", { ...value.footer, text: e.target.value })}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="embed-footer-icon">Footer icon URL</Label>
                <Input
                  id="embed-footer-icon"
                  value={value.footer.iconUrl ?? ""}
                  onChange={(e) => set("footer", { ...value.footer, iconUrl: e.target.value })}
                />
              </div>
            </div>
          </div>
        </details>

        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <Label>Fields</Label>
            <Button type="button" variant="ghost" size="sm" onClick={addField} disabled={value.fields.length >= 25}>
              <Plus aria-hidden /> Add field
            </Button>
          </div>
          {value.fields.length === 0 && (
            <FieldHint>Fields are the labelled columns beneath the description. Up to 25.</FieldHint>
          )}
          {value.fields.map((field, index) => (
            <Card key={index} className="space-y-3 p-3">
              <div className="flex items-center gap-2">
                <GripVertical className="size-4 shrink-0 text-fg-subtle" aria-hidden />
                <Input
                  value={field.name}
                  maxLength={256}
                  placeholder="Field name"
                  onChange={(e) => setField(index, { name: e.target.value })}
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  onClick={() => removeField(index)}
                  aria-label="Remove field"
                >
                  <Trash2 aria-hidden />
                </Button>
              </div>
              <Textarea
                value={field.value}
                maxLength={1024}
                rows={2}
                placeholder="Field value"
                onChange={(e) => setField(index, { value: e.target.value })}
              />
              <label className="flex items-center gap-2 text-sm text-fg-muted">
                <Switch
                  checked={field.inline}
                  onCheckedChange={(checked) => setField(index, { inline: checked })}
                />
                Show inline (side by side)
              </label>
            </Card>
          ))}
        </div>
      </div>

      <EmbedPreview embed={value} colorHex={colorHex} />
    </div>
  );
}

/** A compact, Discord-like preview so the admin sees roughly what will post. */
function EmbedPreview({ embed, colorHex }: { embed: Embed; colorHex: string }) {
  const empty =
    !embed.title &&
    !embed.description &&
    !embed.imageUrl &&
    !embed.author.name &&
    !embed.footer.text &&
    embed.fields.length === 0;

  return (
    <div className="lg:sticky lg:top-4 lg:self-start">
      <Label className="mb-2 block">Preview</Label>
      <div className="rounded-lg bg-[#313338] p-3 text-sm text-[#dbdee1]">
        {empty ? (
          <p className="py-6 text-center text-xs text-[#949ba4]">Nothing to preview yet.</p>
        ) : (
          <div
            className="rounded border-l-4 bg-[#2b2d31] p-3"
            style={{ borderColor: embed.color !== null ? colorHex : "#4e5058" }}
          >
            {embed.author.name && (
              <div className="mb-1 text-xs font-semibold text-white">{embed.author.name}</div>
            )}
            {embed.title && (
              <div className="mb-1 font-semibold text-[#00a8fc]">{embed.title}</div>
            )}
            {embed.description && (
              <div className="whitespace-pre-wrap break-words text-[13px] leading-snug text-[#dbdee1]">
                {embed.description}
              </div>
            )}
            {embed.fields.length > 0 && (
              <div className="mt-2 grid grid-cols-1 gap-2">
                {embed.fields.map((f, i) => (
                  <div key={i}>
                    <div className="text-xs font-semibold text-white">{f.name || "​"}</div>
                    <div className="whitespace-pre-wrap break-words text-xs text-[#dbdee1]">{f.value || "​"}</div>
                  </div>
                ))}
              </div>
            )}
            {embed.imageUrl && (
              // eslint-disable-next-line @next/next/no-img-element
              <img
                src={embed.imageUrl}
                alt=""
                className="mt-2 max-h-40 rounded object-cover"
                onError={(e) => ((e.target as HTMLImageElement).style.display = "none")}
              />
            )}
            {embed.footer.text && (
              <div className="mt-2 text-xs text-[#949ba4]">{embed.footer.text}</div>
            )}
          </div>
        )}
      </div>
      <FieldHint className="mt-2">
        A role ping only notifies when it&apos;s in the text above the embed, never inside it.
      </FieldHint>
    </div>
  );
}
