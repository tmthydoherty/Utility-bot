/**
 * JSON parsing that does not quietly destroy Discord IDs.
 *
 * A snowflake is a 64-bit integer. `Number.MAX_SAFE_INTEGER` is 2^53-1, and
 * every Discord ID minted since 2015 is larger than that — so `JSON.parse`
 * silently rounds it:
 *
 *     JSON.parse('[1484394129927438428]')[0]  ->  1484394129927438300
 *
 * No error, no warning, and the last three digits are simply wrong.
 *
 * The Discord panel writes channel and role IDs into an automation's config as
 * bare JSON numbers, because Python integers have no such limit. Read those
 * with a plain `JSON.parse` and two things happen: the ID never matches a real
 * channel, so the builder shows a raw number instead of `#general` — and far
 * worse, saving that automation back writes the *rounded* ID into the bot's
 * database, silently repointing a live rule at a channel that does not exist.
 * That is data loss caused by opening a page.
 *
 * A `JSON.parse` reviver cannot fix this: by the time the reviver is called the
 * number has already been parsed and the precision is gone. So the digits are
 * quoted in the *text* before parsing, which turns every oversized integer into
 * a string and leaves it byte-for-byte intact. The Python side coerces with
 * `int(v)` (see `_ids` in conditions.py), so a string ID is read identically to
 * a numeric one — and everything this dashboard writes uses strings anyway.
 */

/** Below this many digits a bare integer cannot be a snowflake. */
const SNOWFLAKE_DIGITS = 15;

/**
 * Quote every oversized bare integer, without touching string contents.
 *
 * Written as a scanner rather than a regex because a regex cannot tell a number
 * in value position from the same digits inside a message body — and an
 * automation that posts "call 12345678901234567890" is unusual but perfectly
 * legal, and must not have its own text rewritten.
 */
export function quoteBigIntegers(text: string): string {
  let out = "";
  let index = 0;
  let inString = false;

  while (index < text.length) {
    const char = text[index]!;

    if (inString) {
      out += char;
      if (char === "\\") {
        // Copy the escaped character wholesale, so an escaped quote does not
        // read as the end of the string.
        index += 1;
        if (index < text.length) out += text[index]!;
      } else if (char === '"') {
        inString = false;
      }
      index += 1;
      continue;
    }

    if (char === '"') {
      inString = true;
      out += char;
      index += 1;
      continue;
    }

    if (char >= "0" && char <= "9") {
      // A digit run only starts a number token when what precedes it is
      // structural. Otherwise it is part of something already being copied.
      const start = index;
      while (index < text.length && text[index]! >= "0" && text[index]! <= "9") index += 1;
      const digits = text.slice(start, index);
      const next = text[index];

      // A fractional or exponent part means it is not an ID; leave it alone.
      const isWholeNumber = next !== "." && next !== "e" && next !== "E";
      const isNegative = out.endsWith("-");

      if (isWholeNumber && !isNegative && digits.length >= SNOWFLAKE_DIGITS) {
        out += `"${digits}"`;
      } else {
        out += digits;
      }
      continue;
    }

    out += char;
    index += 1;
  }

  return out;
}

/** `JSON.parse`, with Discord IDs surviving the trip. */
export function parseJsonPreservingIds<T>(raw: string | null | undefined, fallback: T): T {
  if (!raw) return fallback;
  try {
    return JSON.parse(quoteBigIntegers(raw)) as T;
  } catch {
    return fallback;
  }
}
