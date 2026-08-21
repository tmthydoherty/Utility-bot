"use client";

import * as React from "react";

import { cn } from "@/lib/utils";
import { useGuild } from "@/components/providers/guild-provider";
import { EmojiGlyph } from "@/components/dashboard/emoji-glyph";
import {
  automationSentence,
  parseEmojiToken,
  sentenceText,
  type Fragment,
  type Namer,
} from "@/lib/automations/sentence";
import type { Automation } from "@/lib/automations/types";

/**
 * The automation, read back as a sentence.
 *
 * This sits at the top of every automation card and above the builder, and it
 * is the thing that makes the whole feature usable by someone who has never
 * set one up. The boxes below tell you *what the settings are*; this tells you
 * *what it does*, which is the only question anyone actually has.
 *
 * Filled-in values are highlighted and blanks show as a dotted "…", so an
 * unfinished automation reads as unfinished in the sentence itself rather than
 * only in a checklist somewhere further down the page.
 */

/** Resolves IDs to the names a person recognises, from the live guild data. */
export function useNamer(): Namer {
  const guild = useGuild();

  return React.useMemo<Namer>(() => {
    const channels = new Map(guild.channels.map((channel) => [channel.id, channel.name]));
    const roles = new Map(guild.roles.map((role) => [role.id, role.name]));
    return {
      // A deleted channel keeps its ID rather than vanishing from the
      // sentence: "#812…" is confusing, but a sentence with a hole in it is
      // worse, and this way the automation is visibly pointing at nothing.
      channel: (id) => `#${channels.get(id) ?? id}`,
      role: (id) => `@${roles.get(id) ?? id}`,
      user: (id) => `@${id}`,
    };
  }, [guild.channels, guild.roles]);
}

export function Fragments({ fragments, className }: { fragments: Fragment[]; className?: string }) {
  return (
    <span className={className}>
      {fragments.map((fragment, index) => {
        if (fragment.kind === "plain") {
          return <React.Fragment key={index}>{fragment.text}</React.Fragment>;
        }
        return (
          <span
            key={index}
            className={cn(
              "rounded px-1 py-0.5",
              fragment.kind === "blank"
                ? // Dotted and warm, so a blank is unmistakably a to-do rather
                  // than a value that happens to be short.
                  "border border-dashed border-[var(--warning)] text-[var(--warning)]"
                : "bg-[var(--accent-soft)] font-medium text-[var(--accent)]",
            )}
          >
            {fragment.emojis ? (
              <span className="inline-flex items-center gap-0.5 align-middle" aria-label={fragment.text}>
                {fragment.emojis.map((token, i) => {
                  const { id, name } = parseEmojiToken(token);
                  return <EmojiGlyph key={`${token}-${i}`} emojiId={id} name={name} size={18} />;
                })}
              </span>
            ) : (
              fragment.text
            )}
          </span>
        );
      })}
    </span>
  );
}

export function AutomationSentence({
  automation,
  className,
  size = "lg",
}: {
  automation: Pick<Automation, "triggerType" | "triggerConfig" | "conditions" | "steps">;
  className?: string;
  size?: "sm" | "lg";
}) {
  const names = useNamer();
  const fragments = React.useMemo(
    () => automationSentence(automation, names),
    [automation, names],
  );

  return (
    <p
      className={cn(
        "text-balance leading-relaxed",
        size === "lg" ? "text-base sm:text-lg" : "text-sm",
        className,
      )}
      // The highlighting is decoration; a screen reader should hear the
      // sentence, not a run of styled spans.
      aria-label={sentenceText(fragments)}
    >
      <Fragments fragments={fragments} />
    </p>
  );
}
