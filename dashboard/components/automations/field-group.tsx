"use client";

import * as React from "react";

import { cn } from "@/lib/utils";
import { FieldType, isVisible, type Field, type FieldValue, type SettingsValues } from "@/lib/schema/types";
import { FieldRow } from "@/components/settings/field-renderer";
import { EmojiGlyph } from "@/components/dashboard/emoji-glyph";
import { useGuild, type GuildEmoji } from "@/components/providers/guild-provider";
import { fetchCrossServerEmojis } from "@/app/actions/emojis";
import { parseEmojiToken } from "@/lib/automations/sentence";

import { PlaceholderHelp } from "./placeholder-help";

/**
 * A catalogue entry's settings, rendered from its field list.
 *
 * Reuses `FieldRow` — the same one every module settings page uses — so an
 * action's options look and behave exactly like the rest of the dashboard, and
 * a new action needs no UI work at all. Visibility is re-evaluated on every
 * keystroke, so the form reshapes as you fill it in: turning "Send it as a
 * fancy box" on grows six more fields, and turning it off puts them away.
 *
 * Two things are handled here rather than in `FieldRow`, because they are
 * specific to automations:
 *
 * * **Emoji lists.** The engine reads emoji as an array (`["✅", "🎉"]`) and
 *   would treat the plain string "✅ 🎉" as one unusable emoji. So a multi
 *   emoji field is typed as text and split on whitespace.
 *
 * * **The placeholder cheat sheet.** Anything that gets *sent* can carry
 *   `{user.mention}` and friends. Nobody discovers those by guessing, so the
 *   list is one tap away from the field it applies to.
 */
export function FieldGroup({
  fields,
  values,
  onChange,
  className,
}: {
  fields: Field[];
  values: SettingsValues;
  onChange: (next: SettingsValues) => void;
  className?: string;
}) {
  const visible = fields.filter((field) => isVisible(field, values));
  if (visible.length === 0) {
    return (
      <p className={cn("text-sm text-fg-subtle", className)}>Nothing to set up — this just works.</p>
    );
  }

  const set = (key: string, value: FieldValue) => onChange({ ...values, [key]: value });
  // Anything that gets sent — the message, a reply, a button's reply — can
  // carry placeholders, so the cheat sheet rides along with those text boxes.
  const isSendable = (field: Field) =>
    field.type === FieldType.MULTILINE &&
    (field.key === "message" || field.key.includes("content"));

  return (
    <div className={cn("grid gap-4 sm:grid-cols-2", className)}>
      {visible.map((field) => {
        if (field.type === FieldType.EMOJI) {
          return (
            <EmojiField
              key={field.key}
              field={field}
              value={values[field.key] ?? null}
              onChange={(next) => set(field.key, next)}
            />
          );
        }

        return (
          <div key={field.key} className={cn(field.wide && "sm:col-span-2")}>
            <FieldRow
              field={field}
              value={values[field.key] ?? null}
              onChange={(next) => set(field.key, next)}
            />
            {isSendable(field) && <PlaceholderHelp />}
          </div>
        );
      })}
    </div>
  );
}

const emojiToken = (emoji: { id: string; name: string; animated: boolean }): string =>
  `<${emoji.animated ? "a" : ""}:${emoji.name}:${emoji.id}>`;

/**
 * An emoji field — a list (a reaction's several emoji) or a single one (a
 * button's).
 *
 * Type or paste unicode emoji straight in. For custom emoji — where the token
 * is `<:name:id>` and nobody knows the id — the picker below lets you tap one
 * instead. A `"guild"` field offers just this server's emoji (all a reaction
 * can use); an `"all"` field offers every server the bot is in, grouped by
 * server, which is what a button can carry. Selections preview as their real
 * glyphs so the field never reads as a wall of raw tokens.
 */
function EmojiField({
  field,
  value,
  onChange,
}: {
  field: Field;
  value: FieldValue;
  onChange: (value: FieldValue) => void;
}) {
  const { emojis, allEmojis } = useGuild();

  // A button can carry an emoji from any server the bot shares. That list is no
  // longer shipped with every page — it's a fan-out of a REST call per server —
  // so pull it once when an "all" field first mounts. Until it lands (and if it
  // fails), the provider's this-server emoji stand in, so the picker is never
  // empty and typing always works.
  const [crossServer, setCrossServer] = React.useState<GuildEmoji[] | null>(null);
  const wantsAll = field.emojiScope === "all";
  React.useEffect(() => {
    if (!wantsAll) return;
    let live = true;
    fetchCrossServerEmojis()
      .then((list) => {
        if (live && list.length) setCrossServer(list);
      })
      .catch(() => {});
    return () => {
      live = false;
    };
  }, [wantsAll]);

  const source = wantsAll ? (crossServer ?? allEmojis) : emojis;
  const isMulti = !!field.multi;

  const list = Array.isArray(value) ? value : [];
  const initialText = isMulti ? list.join(" ") : (typeof value === "string" ? value : "");
  const [text, setText] = React.useState(initialText);

  // Re-sync when the automation is reloaded underneath us (an undo, a save),
  // but not on every keystroke — that would fight the cursor.
  React.useEffect(() => {
    const canonical = isMulti
      ? list.join(" ")
      : (typeof value === "string" ? value : "");
    setText((current) =>
      current.trim().split(/\s+/).filter(Boolean).join(" ") === canonical.trim()
        ? current
        : canonical,
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value]);

  const commit = (next: string) => {
    setText(next);
    const tokens = next.split(/\s+/).filter(Boolean);
    if (isMulti) {
      onChange(tokens.slice(0, 25));
    } else {
      // A single field keeps only the first emoji, stored as a plain string.
      onChange(tokens[0] ?? null);
    }
  };

  const pick = (token: string) => {
    if (isMulti) {
      if (list.length >= 25) return;
      commit(`${text.trim()} ${token}`.trim());
    } else {
      commit(token);
    }
  };

  // Group the picker by server only when it spans several of them.
  const groups = React.useMemo<{ name: string; emojis: typeof source }[]>(() => {
    if (field.emojiScope !== "all") return [{ name: "", emojis: source }];
    const byServer = new Map<string, typeof source>();
    for (const emoji of source) {
      const key = emoji.guildName ?? "";
      const bucket = byServer.get(key);
      if (bucket) bucket.push(emoji);
      else byServer.set(key, [emoji]);
    }
    return Array.from(byServer, ([name, list]) => ({ name, emojis: list }));
  }, [source, field.emojiScope]);

  return (
    <div className={cn("space-y-2", field.wide && "sm:col-span-2")}>
      <label htmlFor={`emoji-${field.key}`} className="text-sm font-medium">
        {field.label}
      </label>
      <input
        id={`emoji-${field.key}`}
        value={text}
        onChange={(event) => commit(event.target.value)}
        placeholder={isMulti ? "👍 👎 🎉" : "🎉"}
        className={cn(
          "h-11 w-full rounded-md border border-[var(--border)] bg-[var(--bg-inset)] px-3",
          "text-base outline-none transition-colors placeholder:text-fg-subtle",
          "focus-visible:border-[var(--accent)] focus-visible:ring-2 focus-visible:ring-[var(--ring)]",
        )}
      />
      {field.help && <p className="text-xs text-fg-subtle">{field.help}</p>}

      {source.length > 0 && (
        <div className="space-y-1.5">
          <p className="text-xs text-fg-subtle">
            {isMulti ? "Tap a custom emoji to add it" : "Tap a custom emoji to use it"}
          </p>
          <div className="max-h-40 space-y-2 overflow-y-auto rounded-md border border-[var(--border)] bg-[var(--bg-inset)] p-2">
            {groups.map((group) => (
              <div key={group.name || "_"} className="space-y-1">
                {group.name && (
                  <p className="px-0.5 text-[11px] font-medium uppercase tracking-wide text-fg-subtle">
                    {group.name}
                  </p>
                )}
                <div className="flex flex-wrap gap-1">
                  {group.emojis.map((emoji) => {
                    const token = emojiToken(emoji);
                    const chosen = !isMulti && text.trim() === token;
                    return (
                      <button
                        key={`${group.name}-${emoji.id}`}
                        type="button"
                        onClick={() => pick(token)}
                        title={`:${emoji.name}:`}
                        aria-label={`Use :${emoji.name}:`}
                        className={cn(
                          "grid size-8 shrink-0 place-items-center rounded transition-colors",
                          "hover:bg-[var(--surface-hover)] focus-visible:outline-none",
                          "focus-visible:ring-2 focus-visible:ring-[var(--ring)]",
                          chosen && "bg-[var(--accent-soft)] ring-2 ring-[var(--accent)]",
                        )}
                      >
                        <EmojiGlyph emojiId={emoji.id} name={emoji.name} size={22} />
                      </button>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {isMulti && list.length > 0 && (
        <p className="flex flex-wrap items-center gap-1 text-xs text-fg-muted">
          <span>{list.length} emoji, added left to right:</span>
          {list.map((token, index) => {
            const { id, name } = parseEmojiToken(token);
            return <EmojiGlyph key={`${token}-${index}`} emojiId={id} name={name} size={18} />;
          })}
        </p>
      )}

      {!isMulti && text.trim() && (
        <p className="flex flex-wrap items-center gap-1 text-xs text-fg-muted">
          <span>On the button:</span>
          {(() => {
            const { id, name } = parseEmojiToken(text.trim());
            return <EmojiGlyph emojiId={id} name={name} size={18} />;
          })()}
          <button
            type="button"
            onClick={() => commit("")}
            className="ml-1 text-fg-subtle underline-offset-2 hover:underline"
          >
            clear
          </button>
        </p>
      )}
    </div>
  );
}
