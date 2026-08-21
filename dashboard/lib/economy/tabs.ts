import { economy } from "@/lib/schema/modules";
import type { Section } from "@/lib/schema/types";

/**
 * The Economy section, split into three settings tabs plus the member
 * inspector.
 *
 * The tabs are a re-presentation of the one `economy` module schema — no field
 * is duplicated or redefined. Each tab names the section ids it shows, and the
 * panel that renders it still holds the *whole* module's values, so a save from
 * any tab writes a complete, consistent set and `visibleWhen` conditions that
 * reference a field on another tab keep resolving.
 *
 * If a section id here ever stops matching one in cogs/economy (via
 * lib/schema/modules.ts), `sectionsFor` simply omits it — the page renders the
 * sections it found rather than throwing.
 */

export interface EconomyTab {
  key: "points" | "shop" | "config";
  /** Path suffix under `/dashboard/<guildId>/economy`. */
  slug: string;
  title: string;
  /** One line, shown on the landing tile and the tab's own header. */
  description: string;
  /** lucide-react icon name, resolved through components/ui/icon.tsx. */
  icon: string;
  /** Section ids from the economy schema, in display order. */
  sectionIds: string[];
}

export const ECONOMY_TABS: EconomyTab[] = [
  {
    key: "points",
    slug: "points",
    title: "Points",
    description: "What each activity pays and its daily cap.",
    icon: "Coins",
    sectionIds: ["earning"],
  },
  {
    key: "shop",
    slug: "shop",
    title: "Shop",
    description: "Every item members can buy — price, availability and how long it lasts.",
    icon: "Tag",
    sectionIds: [
      "shop_availability",
      "shop_pricing",
      "gif_commands",
      "timings",
      "box_odds",
    ],
  },
  {
    key: "config",
    slug: "config",
    title: "Config",
    description: "Roles, channels and the curse timings the Mystery Box uses.",
    icon: "SlidersHorizontal",
    sectionIds: ["roles", "channels", "curses"],
  },
];

/** The audit/inspector tile — not a settings tab, so kept separate. */
export const ECONOMY_AUDIT_TILE = {
  slug: "audit",
  title: "Audit log",
  description: "Look up any member: their balance, items and how they earned every point.",
  icon: "ScrollText",
} as const;

export function economyTab(key: EconomyTab["key"]): EconomyTab {
  const tab = ECONOMY_TABS.find((t) => t.key === key);
  if (!tab) throw new Error(`Unknown economy tab: ${key}`);
  return tab;
}

/** The schema sections a tab shows, in the tab's declared order. */
export function sectionsFor(tab: EconomyTab): Section[] {
  const byId = new Map((economy.sections ?? []).map((section) => [section.id, section]));
  return tab.sectionIds
    .map((id) => byId.get(id))
    .filter((section): section is Section => section !== undefined);
}
