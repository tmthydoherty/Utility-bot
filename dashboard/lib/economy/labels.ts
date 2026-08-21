/**
 * Plain-English names for the economy's internal keys.
 *
 * A mirror of the catalogues in cogs/economy/config.py, kept verbatim the same
 * way the settings schema mirrors the bot's config keys: the inspector reads
 * raw `source` strings out of the ledger and `item_key`s out of inventory, and
 * these turn them into something a person can read. If the bot gains a source
 * or an item, add it here too — an unknown key degrades to a tidied version of
 * itself rather than breaking the page.
 */

/** Points earning sources — the keys the ledger stores for a credit. */
export const EARNING_LABELS: Record<string, string> = {
  message: "Messages",
  first_message: "First message of the day",
  streak7: "7-day streak bonus",
  newcomer_reply: "Replying to newcomers",
  trivia: "Daily trivia",
  daily_quote: "Daily quote",
  qotd: "Question of the Day",
  custom_match: "Custom matches",
  game_night: "Game night",
  mystery_box: "Mystery Box",
};

/** Every item that can sit in inventory — shop items and box-only items. */
export const ITEM_LABELS: Record<string, string> = {
  auto_react: "Auto React",
  hof_post: "HoF Post",
  gif_command: "Add GIF Command",
  add_emoji: "Add Emoji",
  throne: "The Throne",
  customs_match: "Customs Match",
  proxy: "Proxy",
  mystery_box: "Mystery Box",
  nickname_hijack: "Nickname Hijack",
  curse_wipe: "Curse Wipe",
};

/** Turn a raw key into a spaced, capitalised fallback: `game_night` → `Game night`. */
function humanise(key: string): string {
  const spaced = key.replace(/[_:]/g, " ").trim();
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

export function itemLabel(itemKey: string): string {
  return ITEM_LABELS[itemKey] ?? humanise(itemKey);
}

/**
 * A readable label for a ledger `source`.
 *
 * Credits carry an earning source (or `mystery_box`); spends carry
 * `shop:<item>`; refunds carry `refund:<reason>`; admin grants carry `admin`.
 */
export function sourceLabel(source: string): string {
  const known = EARNING_LABELS[source];
  if (known) return known;
  if (source === "admin") return "Admin adjustment";
  if (source.startsWith("shop:")) return `Bought ${itemLabel(source.slice(5))}`;
  if (source.startsWith("refund:")) return "Refund";
  return humanise(source);
}

/**
 * The bucket a positive ledger entry belongs in for the earning breakdown.
 *
 * The breakdown groups by earning source; a refund or an admin grant is still
 * a way points arrived, so they get their own readable buckets rather than
 * being dropped or lumped under an item name.
 */
export function earningBucket(source: string): string {
  const known = EARNING_LABELS[source];
  if (known) return known;
  if (source === "admin") return "Admin adjustment";
  if (source.startsWith("refund:")) return "Refunds";
  return sourceLabel(source);
}
