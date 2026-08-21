import "server-only";

import rawData from "emojibase-data/en/data.json";
import iamcal from "emojibase-data/en/shortcodes/iamcal.json";

/**
 * The label for an emoji row's name column.
 *
 * Custom emoji carry a typed shortcode, so they read as `:name:`. A unicode
 * emoji is stored as its raw glyph (the tracker logs the character itself), so
 * there's no shortcode to show — we look one up from emojibase's data, using the
 * `iamcal` preset because it's the closest to the names Discord itself shows
 * (😭 → `:sob:`, 🫡 → `:saluting_face:`).
 *
 * emojibase is current (it carries the newest emoji and every skin-tone variant,
 * which the older node-emoji dataset missed), but its glyphs always carry a
 * variation selector — so the map is keyed on a normalised form and lookups
 * strip both variation selectors and skin-tone modifiers before giving up. A
 * glyph with no shortcode at all (a brand-new one, or a bare text symbol)
 * returns an empty label rather than repeating the character, which already
 * shows in the row's visual.
 */

interface EmojiDatum {
  hexcode: string;
  emoji?: string;
  text?: string;
  skins?: EmojiDatum[];
}

// U+FE0E / U+FE0F (text vs emoji presentation) and the five skin-tone modifiers.
const VARIATION = /[︎️]/g;
const SKIN_TONE = /[\u{1F3FB}-\u{1F3FF}]/gu;

const stripVariation = (glyph: string) => glyph.replace(VARIATION, "");

/** Built once, lazily — the dataset is ~1MB and only needed the first time an
 *  overview with unicode emoji renders. Keyed on the variation-stripped glyph. */
let glyphToShortcode: Map<string, string> | null = null;

function shortcodeMap(): Map<string, string> {
  if (glyphToShortcode) return glyphToShortcode;

  const shortcodes = iamcal as Record<string, string | string[] | undefined>;
  const pick = (hexcode: string): string | undefined => {
    const value = shortcodes[hexcode];
    return Array.isArray(value) ? value[0] : value;
  };

  const map = new Map<string, string>();
  const put = (glyph: string | undefined, shortcode: string | undefined) => {
    if (!glyph || !shortcode) return;
    const key = stripVariation(glyph);
    if (!map.has(key)) map.set(key, shortcode);
  };

  for (const datum of rawData as unknown as EmojiDatum[]) {
    const shortcode = pick(datum.hexcode);
    put(datum.emoji, shortcode);
    put(datum.text, shortcode);
    // Skin-tone variants rarely have their own shortcode, so they fall back to
    // the base emoji's — good enough, since the tone is visible in the glyph.
    for (const skin of datum.skins ?? []) {
      const skinShortcode = pick(skin.hexcode) ?? shortcode;
      put(skin.emoji, skinShortcode);
      put(skin.text, skinShortcode);
    }
  }

  glyphToShortcode = map;
  return map;
}

/** A unicode emoji's shortcode, trying progressively looser forms of the glyph
 *  (as stored, without variation selectors, without skin tones) before null. */
function shortcodeFor(glyph: string): string | null {
  const map = shortcodeMap();
  const candidates = [
    glyph,
    stripVariation(glyph),
    glyph.replace(SKIN_TONE, ""),
    stripVariation(glyph).replace(SKIN_TONE, ""),
  ];
  for (const candidate of candidates) {
    const hit = map.get(candidate);
    if (hit) return hit;
  }
  return null;
}

export function emojiLabel(emojiId: string | null, name: string): string {
  if (emojiId) return `:${name}:`;
  const short = shortcodeFor(name);
  return short ? `:${short}:` : "";
}
