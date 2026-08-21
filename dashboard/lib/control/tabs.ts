/**
 * The Bot Control section's sub-pages, defined once and consumed by the landing
 * page's tiles. Mirrors lib/economy/tabs.ts in shape so the two sections read
 * and behave the same way.
 *
 * Each tile is a self-contained page under `/dashboard/<guildId>/control`.
 */

export interface ControlTile {
  slug: string;
  title: string;
  /** One line, shown on the landing tile and the sub-page's own header. */
  description: string;
  /** lucide-react icon name, resolved through components/ui/icon.tsx. */
  icon: string;
}

export const CONTROL_TILES: ControlTile[] = [
  {
    slug: "cogs",
    title: "Cogs",
    description: "See which of Vibey's features are loaded, and reload one after a code change.",
    icon: "Blocks",
  },
  {
    slug: "logs",
    title: "Logs",
    description: "The tail of Vibey's log — the latest errors and warnings, without an SSH session.",
    icon: "TerminalSquare",
  },
  {
    slug: "power",
    title: "Power",
    description: "Restart Vibey, or reload every feature at once. Owner-only, with a confirm.",
    icon: "Power",
  },
  {
    slug: "presence",
    title: "Presence",
    description: "The status dot and activity line Vibey shows on its own profile.",
    icon: "Activity",
  },
];
