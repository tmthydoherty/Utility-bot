/**
 * An automation, read back as an English sentence.
 *
 * This is the single most important thing on the page. Every automation
 * builder in existence shows you three boxes — a trigger, some conditions,
 * some actions — and leaves you to hold the sentence in your head. That works
 * fine for the person who built the thing and badly for everyone else, and it
 * is why people end up with automations they are afraid to touch.
 *
 * So the builder shows the sentence first and the boxes underneath:
 *
 *   When someone sends a message, if the message contains "hello"
 *   and they don't have @Muted, reply "Hey there!"
 *
 * If that reads wrong, the automation is wrong, and you can see it without
 * understanding a single thing about how any of this works.
 *
 * Values are rendered the way a person would say them — a channel as #general,
 * a role as @Members, 600 as "10 minutes" — which is why this needs a
 * `resolve` for the names it cannot know on its own.
 */

import { FieldType, isVisible, type Field, type FieldValue, type SettingsValues } from "@/lib/schema/types";
import { formatDuration } from "@/lib/utils";

import { ACTIONS, CONDITIONS, TRIGGERS, type Phrase } from "./registry";
import {
  isBranch,
  isGroup,
  type Automation,
  type ConditionGroup,
  type ConditionLeaf,
  type ConditionNode,
  type Step,
} from "./types";

/** Turns an ID into the name a person would recognise. */
export interface Namer {
  channel: (id: string) => string;
  role: (id: string) => string;
  user: (id: string) => string;
}

/** Falls back to the raw ID, so a deleted channel still reads as *something*. */
export const RAW_NAMES: Namer = {
  channel: (id) => `#${id}`,
  role: (id) => `@${id}`,
  user: (id) => `@${id}`,
};

/**
 * A fragment of the sentence, tagged so it can be styled and clicked.
 *
 * Returned as pieces rather than one string because the whole point is that
 * each part of the sentence is a link to the setting behind it — reading the
 * sentence and editing the automation are the same gesture.
 */
export interface Fragment {
  text: string;
  /** "value" is a filled-in setting; "blank" is one still to be decided. */
  kind: "plain" | "value" | "blank";
  /**
   * Emoji tokens to draw as their real glyphs in place of `text`. Custom emoji
   * are stored as `<:name:id>`, which reads as gibberish spelled out; the
   * renderer swaps them for the image, and `text` stays the readable fallback
   * a screen reader hears.
   */
  emojis?: string[];
}

const BLANK = "…";

/**
 * One emoji token, split for display. A custom emoji is `<:name:id>` (or the
 * animated `<a:name:id>`); anything else is a unicode character the font draws.
 */
export function parseEmojiToken(token: string): { id: string | null; name: string } {
  const match = /^<a?:([a-zA-Z0-9_]+):(\d+)>$/.exec(token);
  if (!match) return { id: null, name: token };
  return { id: match[2] ?? null, name: match[1] ?? token };
}

/** The emoji field's value as a clean token list. */
function emojiList(value: FieldValue | undefined): string[] {
  const list = Array.isArray(value) ? value : value ? [value] : [];
  return list.map((entry) => String(entry)).filter(Boolean);
}

function renderValue(field: Field, value: FieldValue, names: Namer): string {
  if (value === null || value === undefined || value === "") return BLANK;

  const list = Array.isArray(value) ? value : [value];
  if (list.length === 0) return BLANK;

  switch (field.type) {
    case FieldType.CHANNEL:
      return joinNicely(list.map((id) => names.channel(String(id))));
    case FieldType.ROLE:
      return joinNicely(list.map((id) => names.role(String(id))));
    case FieldType.USER:
      return joinNicely(list.map((id) => names.user(String(id))));
    case FieldType.DURATION: {
      const seconds = Number(value);
      return seconds > 0 ? formatDuration(seconds) : BLANK;
    }
    case FieldType.BOOL:
      return value ? "yes" : "no";
    case FieldType.CHOICE: {
      const labels = list.map((entry) => {
        const choice = field.choices?.find((c) => c.value === String(entry));
        return choice?.label ?? String(entry);
      });
      return joinNicely(labels);
    }
    case FieldType.EMOJI:
      return list.join(" ");
    case FieldType.MULTILINE:
    case FieldType.TEXT: {
      const text = String(value).replace(/\s+/g, " ").trim();
      if (!text) return BLANK;
      // Long message bodies are the one thing that would swamp the sentence,
      // so they are clipped here and shown in full in the step below.
      return text.length > 48 ? `“${text.slice(0, 47)}…”` : `“${text}”`;
    }
    default:
      return String(value);
  }
}

/** "a, b and c" — an Oxford-free list, because it is being read aloud. */
function joinNicely(items: string[]): string {
  if (items.length === 0) return BLANK;
  if (items.length === 1) return items[0]!;
  if (items.length <= 3) return `${items.slice(0, -1).join(", ")} and ${items.at(-1)}`;
  return `${items.slice(0, 2).join(", ")} and ${items.length - 2} more`;
}

/**
 * Fill a phrase template's `{key}` tokens from a config.
 *
 * Tokens that resolve to nothing come back as `…` marked "blank", which is how
 * an unfinished automation reads as unfinished in the sentence itself rather
 * than only in a checklist somewhere else.
 */
function fillPhrase(
  template: string,
  fields: Field[],
  config: SettingsValues,
  names: Namer,
): Fragment[] {
  const fragments: Fragment[] = [];
  const pattern = /\{([a-z_0-9.]+)\}/gi;
  let lastIndex = 0;
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(template)) !== null) {
    if (match.index > lastIndex) {
      fragments.push({ text: template.slice(lastIndex, match.index), kind: "plain" });
    }
    const key = match[1]!;
    const field = fields.find((candidate) => candidate.key === key);

    if (field?.type === FieldType.EMOJI) {
      const tokens = emojiList(config[key]);
      if (tokens.length === 0) {
        fragments.push({ text: BLANK, kind: "blank" });
      } else {
        // Readable text (`:name:` for custom, the glyph itself for unicode) is
        // the screen-reader fallback; the renderer draws the images from tokens.
        const readable = tokens
          .map((token) => (parseEmojiToken(token).id ? `:${parseEmojiToken(token).name}:` : token))
          .join(" ");
        fragments.push({ text: readable, kind: "value", emojis: tokens });
      }
      lastIndex = match.index + match[0].length;
      continue;
    }

    const rendered = field ? renderValue(field, config[key] ?? null, names) : BLANK;
    fragments.push({ text: rendered, kind: rendered === BLANK ? "blank" : "value" });
    lastIndex = match.index + match[0].length;
  }
  if (lastIndex < template.length) {
    fragments.push({ text: template.slice(lastIndex), kind: "plain" });
  }
  return fragments;
}

function pickPhrase(
  phrase: Phrase[] | string,
  config: SettingsValues,
): string {
  if (typeof phrase === "string") return phrase;
  for (const option of phrase) {
    if (!option.when) return option.text;
    // Reuses the same evaluator the field visibility rules use, so a phrase
    // condition and a `visibleWhen` can never drift apart in meaning.
    if (isVisible({ key: "", label: "", type: FieldType.TEXT, visibleWhen: option.when }, config)) {
      return option.text;
    }
  }
  return phrase.at(-1)?.text ?? "";
}

export function triggerPhrase(
  triggerType: string,
  config: SettingsValues,
  names: Namer = RAW_NAMES,
): Fragment[] {
  const spec = TRIGGERS[triggerType];
  if (!spec) return [{ text: "something happens", kind: "blank" }];
  return fillPhrase(pickPhrase(spec.phrase, config), spec.fields ?? [], config, names);
}

/** How many list values a config field actually holds. */
function valueCount(value: FieldValue | undefined): number {
  if (value === null || value === undefined || value === "") return 0;
  if (Array.isArray(value)) return value.filter((item) => item !== null && item !== undefined && item !== "").length;
  return 1;
}

/**
 * "they have the @Exec role" / "they have all of these roles: @A and @B".
 *
 * A single role reads as "the @X role" whatever the match mode, because with
 * one role "all of" and "any of" say the same thing.
 */
function roleHasPhrase(leaf: ConditionLeaf, names: Namer): Fragment[] {
  const field = CONDITIONS.role_has?.fields?.find((candidate) => candidate.key === "roles");
  const value = field ? renderValue(field, leaf.config.roles ?? null, names) : BLANK;
  const valueFrag: Fragment = { text: value, kind: value === BLANK ? "blank" : "value" };
  const match = leaf.config.match;
  if (valueCount(leaf.config.roles) <= 1) {
    const lead = match === "none" ? "they don't have the " : "they have the ";
    return [{ text: lead, kind: "plain" }, valueFrag, { text: " role", kind: "plain" }];
  }
  const lead =
    match === "none"
      ? "they don't have any of these roles: "
      : match === "all"
        ? "they have all of these roles: "
        : "they have one of these roles: ";
  return [{ text: lead, kind: "plain" }, valueFrag];
}

export function conditionPhrase(
  node: ConditionNode,
  names: Namer = RAW_NAMES,
): Fragment[] {
  if (isGroup(node)) {
    if (node.items.length === 0) return [];
    const joiner = node.op === "or" ? " or " : " and ";
    const parts: Fragment[] = [];
    node.items.forEach((child, index) => {
      const rendered = conditionPhrase(child, names);
      if (rendered.length === 0) return;
      if (index > 0 && parts.length > 0) parts.push({ text: joiner, kind: "plain" });
      parts.push(...rendered);
    });
    if (node.negate && parts.length > 0) {
      return [{ text: "it's not the case that ", kind: "plain" }, ...parts];
    }
    return parts;
  }

  const spec = CONDITIONS[node.type];
  if (!spec) return [{ text: `an unknown requirement (${node.type})`, kind: "blank" }];

  // Roles read badly through the generic phrase — "they have all of @Exec"
  // makes @Exec sound like a set. Say it the way a person would, and drop the
  // "all of" for a single role, where "all" and "any" mean the same thing.
  if (node.type === "role_has") {
    const frags = roleHasPhrase(node, names);
    return node.negate ? [{ text: "NOT ", kind: "plain" }, ...frags] : frags;
  }

  const filled = fillPhrase(
    pickPhrase(spec.phrase, node.config),
    spec.fields ?? [],
    node.config,
    names,
  );
  // "doesn't" rather than "NOT (…)": negation is a property of the sentence,
  // not an operator wrapped around it.
  return node.negate ? [{ text: "NOT ", kind: "plain" }, ...filled] : filled;
}

export function actionPhrase(step: Step, names: Namer = RAW_NAMES): Fragment[] {
  if (isBranch(step)) {
    const check = conditionPhrase(step.conditions ?? { op: "and", items: [] }, names);
    return [
      { text: "if ", kind: "plain" },
      ...(check.length ? check : [{ text: BLANK, kind: "blank" as const }]),
      { text: "…", kind: "plain" },
    ];
  }
  const spec = ACTIONS[step.type];
  if (!spec) return [{ text: `an unknown step (${step.type})`, kind: "blank" }];
  return fillPhrase(pickPhrase(spec.phrase, step.config), spec.fields ?? [], step.config, names);
}

function hasValue(value: FieldValue | undefined): boolean {
  if (value === null || value === undefined || value === "") return false;
  if (Array.isArray(value)) return value.some((item) => item !== null && item !== undefined && item !== "");
  return true;
}

/**
 * A role requirement, read into the subject: "someone **with the @Exec role**".
 *
 * Folding it into the subject rather than tacking it on after "if" is what
 * turns a list of clauses joined by "and" into a sentence a person actually
 * says out loud — the whole reason the OR misfire was so easy to miss.
 */
function subjectClause(leaf: ConditionLeaf, names: Namer): Fragment[] {
  const field = CONDITIONS.role_has?.fields?.find((candidate) => candidate.key === "roles");
  if (!field) return [];
  const value = renderValue(field, leaf.config.roles ?? null, names);
  const ids = Array.isArray(leaf.config.roles) ? leaf.config.roles : [leaf.config.roles];
  const many = ids.filter((id) => id !== null && id !== undefined && id !== "").length > 1;
  const lead = many ? (leaf.config.match === "all" ? "with all of " : "with any of ") : "with the ";
  const tail = many ? "" : " role";
  return [
    { text: lead, kind: "plain" },
    { text: value, kind: value === BLANK ? "blank" : "value" },
    { text: tail, kind: "plain" },
  ];
}

/** A channel requirement, read as a place: "…sends a message **in #test**". */
function locationClause(leaf: ConditionLeaf, names: Namer): Fragment[] {
  const field = CONDITIONS.channel_is?.fields?.find((candidate) => candidate.key === "channels");
  if (!field) return [];
  const value = renderValue(field, leaf.config.channels ?? null, names);
  const parts: Fragment[] = [
    { text: " in ", kind: "plain" },
    { text: value, kind: value === BLANK ? "blank" : "value" },
  ];
  if (leaf.config.include_threads === true) {
    parts.push({ text: " or a thread inside it", kind: "plain" });
  }
  return parts;
}

/**
 * Splice the role clause in after "someone", so the subject reads as one
 * phrase. Returns null when the trigger has no "someone" to attach to — a
 * timer or "a message is deleted" — in which case the role stays an ordinary
 * requirement rather than being forced somewhere it doesn't fit.
 */
function weaveSubject(triggerFrags: Fragment[], subject: Fragment[]): Fragment[] | null {
  const index = triggerFrags.findIndex(
    (fragment) => fragment.kind === "plain" && /^someone\b/.test(fragment.text),
  );
  if (index === -1) return null;
  const rest = triggerFrags[index]!.text.replace(/^someone\s*/, "");
  return [
    ...triggerFrags.slice(0, index),
    { text: "someone ", kind: "plain" },
    ...subject,
    { text: rest ? ` ${rest}` : "", kind: "plain" },
    ...triggerFrags.slice(index + 1),
  ];
}

/** The whole automation as one sentence, in the order it happens. */
export function automationSentence(
  automation: Pick<Automation, "triggerType" | "triggerConfig" | "conditions" | "steps">,
  names: Namer = RAW_NAMES,
): Fragment[] {
  const root = automation.conditions;
  // Only a plain top-level AND folds into the trigger clause. An OR ("any one
  // is enough") or a negated group has to stay spelled out after "if", because
  // weaving it into the subject would quietly change what it means.
  const foldable = isGroup(root) && root.op === "and" && !root.negate;

  let triggerFrags = triggerPhrase(automation.triggerType, automation.triggerConfig, names);
  const parts: Fragment[] = [{ text: "When ", kind: "plain" }];

  if (foldable) {
    let subject: ConditionLeaf | null = null;
    let location: ConditionLeaf | null = null;
    const rest: ConditionNode[] = [];
    for (const item of root.items) {
      if (!subject && !isGroup(item) && !item.negate && item.type === "role_has"
          && item.config.match !== "none" && hasValue(item.config.roles)) {
        subject = item;
      } else if (!location && !isGroup(item) && !item.negate && item.type === "channel_is"
          && hasValue(item.config.channels)) {
        location = item;
      } else {
        rest.push(item);
      }
    }

    let subjectFolded = false;
    if (subject) {
      const woven = weaveSubject(triggerFrags, subjectClause(subject, names));
      if (woven) {
        triggerFrags = woven;
        subjectFolded = true;
      }
    }
    parts.push(...triggerFrags);
    if (location) parts.push(...locationClause(location, names));

    // A role that couldn't be woven into the subject stays a normal requirement
    // rather than vanishing from the sentence.
    if (subject && !subjectFolded) rest.unshift(subject);

    const remaining = conditionPhrase({ op: "and", items: rest } as ConditionGroup, names);
    if (remaining.length > 0) {
      const wovenSomething = subjectFolded || location !== null;
      parts.push({ text: wovenSomething ? " and " : ", if ", kind: "plain" });
      parts.push(...remaining);
    }
  } else {
    parts.push(...triggerFrags);
    const checks = conditionPhrase(root, names);
    if (checks.length > 0) {
      parts.push({ text: ", if ", kind: "plain" });
      parts.push(...checks);
    }
  }

  if (automation.steps.length === 0) {
    parts.push({ text: ", then ", kind: "plain" });
    parts.push({ text: "do nothing yet", kind: "blank" });
    return withFullStop(parts);
  }

  parts.push({ text: ", then ", kind: "plain" });
  // Only the top level. A branch's contents belong in the step list, not
  // inlined into a sentence that would stop being one.
  automation.steps.forEach((step, index) => {
    if (index > 0) {
      parts.push({ text: index === automation.steps.length - 1 ? " and " : ", ", kind: "plain" });
    }
    parts.push(...actionPhrase(step, names));
  });

  return withFullStop(parts);
}

function withFullStop(parts: Fragment[]): Fragment[] {
  return [...parts, { text: ".", kind: "plain" }];
}

/** The sentence as plain text, for a card, a tooltip or a screen reader. */
export function sentenceText(fragments: Fragment[]): string {
  return fragments.map((fragment) => fragment.text).join("");
}
