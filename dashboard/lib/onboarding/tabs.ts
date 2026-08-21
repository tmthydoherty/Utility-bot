/**
 * Welcome & Onboarding, split into tiles.
 *
 * The module keeps its place in the Modules grid, but its own page is a landing
 * of tiles — one per section — rather than a single long scroll. Each tile links
 * to a sub-page under `/dashboard/<guildId>/modules/welcome/<slug>`. This mirrors
 * the Economy section's tile landing; the difference is that Welcome stays a
 * module (it isn't relocated out of the grid).
 *
 * The `settings` tile is the flat schema form (greeting, introductions, roles,
 * tiers, points). The other four are the rich-state panels that aren't flat
 * settings — the question pool, the game→LFG mappings, per-member points and the
 * blacklist — each driven by the snapshot+commands bridge in
 * lib/onboarding/store.ts.
 */

export interface OnboardingTile {
  /** Path suffix under `/dashboard/<guildId>/modules/welcome`. */
  slug: string;
  title: string;
  /** One line, shown on the landing tile and the sub-page's own header. */
  description: string;
  /** lucide-react icon name, resolved through components/ui/icon.tsx. */
  icon: string;
}

export const ONBOARDING_TILES: OnboardingTile[] = [
  {
    slug: "settings",
    title: "Settings",
    description: "The greeting, introductions, roles, tiers and what replies are worth.",
    icon: "SlidersHorizontal",
  },
  {
    slug: "questions",
    title: "Introduction questions",
    description: "The questions asked in the Introduce Yourself form, in order.",
    icon: "MessageCircleQuestion",
  },
  {
    slug: "mappings",
    title: "Game → LFG mappings",
    description: "Pair a game role with the LFG thread the greeting points a new member to.",
    icon: "Link2",
  },
  {
    slug: "points",
    title: "Points",
    description: "Edit a member's points, see who's where, and read the ranked lists.",
    icon: "Coins",
  },
  {
    slug: "blacklist",
    title: "Blacklist",
    description: "Members blocked from using the Introduce Yourself button.",
    icon: "Ban",
  },
];

export function onboardingTile(slug: string): OnboardingTile {
  const tile = ONBOARDING_TILES.find((t) => t.slug === slug);
  if (!tile) throw new Error(`Unknown onboarding tile: ${slug}`);
  return tile;
}
