export interface RolesAndAlertsTile {
  slug: string;
  title: string;
  description: string;
  icon: string;
}

export const ROLES_AND_ALERTS_TILES: RolesAndAlertsTile[] = [
  {
    slug: "settings",
    title: "Settings",
    description: "The channel and banner images for the alerts & colors.",
    icon: "SlidersHorizontal",
  },
  {
    slug: "tiers",
    title: "Gate & VIP Roles",
    description: "The required tier roles and their associated VIP roles.",
    icon: "ShieldAlert",
  },
  {
    slug: "colors",
    title: "Name Colors",
    description: "The selectable name colors for Tier 1, Tier 2, and Tier 3.",
    icon: "Palette",
  },
  {
    slug: "alerts",
    title: "Alert Roles",
    description: "The ping roles any member can toggle on or off.",
    icon: "Bell",
  },
  {
    slug: "shuffle",
    title: "Role Shuffle",
    description: "Bulk assign or remove a role from members with specific trigger roles.",
    icon: "Shuffle",
  },
];

export function rolesAndAlertsTile(slug: string): RolesAndAlertsTile {
  const tile = ROLES_AND_ALERTS_TILES.find((t) => t.slug === slug);
  if (!tile) throw new Error(`Unknown Roles & Alerts tile: ${slug}`);
  return tile;
}
