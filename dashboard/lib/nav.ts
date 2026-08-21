import { CATEGORY_LABELS, CATEGORY_ORDER, type ModuleSchema } from "@/lib/schema/types";
import { MODULES } from "@/lib/schema/modules";

/**
 * The navigation model, defined once and consumed by the sidebar, the mobile
 * tab bar, the breadcrumbs and the command palette. Four presentations of one
 * list — if they were four lists they would disagree within a month.
 */

export interface NavItem {
  href: string;
  label: string;
  icon: string;
  /** Shown in the desktop sidebar. Ticketing is deliberately not — it reaches
   *  people through its Modules tile — and the operator sections live in
   *  Settings instead (see {@link secondary}). */
  sidebar?: boolean;
  /** Shown in the mobile tab bar. Anything else lives behind "More". */
  primary?: boolean;
  /**
   * Grouped under Settings → "Other sections" rather than getting a top-level
   * sidebar slot. These are the operator-facing sections (Audit log, Bot
   * control) that people open least often, kept off the main rail on every
   * viewport, not just the phone.
   */
  secondary?: boolean;
  /**
   * Only the bot owner may see or reach this. Filtered out of every nav
   * presentation unless `includeOwner` is passed — see the comment on
   * `navItems`. The pages behind such an item must still enforce ownership
   * themselves; hiding a link is not access control.
   */
  ownerOnly?: boolean;
  description?: string;
}

/**
 * The nav model.
 *
 * `includeOwner` gates the owner-only items (Bot Control): pass the viewer's
 * ownership so admins who aren't the owner never see them. It defaults to false
 * so any caller that forgets fails closed rather than leaking the link.
 */
export function navItems(guildId: string, includeOwner = false): NavItem[] {
  const base = `/dashboard/${guildId}`;
  const items: NavItem[] = [
    {
      href: base,
      label: "Overview",
      icon: "LayoutDashboard",
      sidebar: true,
      primary: true,
      description: "Server health at a glance",
    },
    {
      href: `${base}/automations`,
      label: "Automations",
      icon: "Zap",
      // Off the sidebar deliberately: it reaches people through its Modules
      // tile (like Ticketing) rather than a top-level rail slot. It stays in
      // the command palette so a search still lands on it.
      description: "Make Vibey do things by itself",
    },
    {
      href: `${base}/ticketing`,
      label: "Ticketing",
      icon: "TicketCheck",
      // Neither on the sidebar nor in the tab bar: it's really a module, so its
      // front door is its tile on the Modules page. It stays in the command
      // palette so a search still lands on it.
      description: "Panels, topics and responses",
    },
    {
      href: `${base}/custom-matches`,
      label: "Custom Matches",
      icon: "Swords",
      // Neither on the sidebar nor the tab bar: like Ticketing it's really a
      // module, so its front door is its tile on the Modules page. It stays in
      // the command palette so a search still lands on it.
      description: "In-house matchmaking, MMR and leaderboards",
    },
    {
      href: `${base}/modules`,
      label: "Modules",
      icon: "Blocks",
      sidebar: true,
      primary: true,
      description: "Turn features on and configure them",
    },
    {
      href: `${base}/economy`,
      label: "Economy",
      icon: "Coins",
      sidebar: true,
      // Primary: it earns its own icon in the mobile tab bar (right of Modules)
      // rather than being tucked into the Settings page's "Other sections" list,
      // where a non-primary section otherwise lands on a phone.
      primary: true,
      description: "Points, the shop, and every member's balance",
    },
    {
      href: `${base}/audit`,
      label: "Audit log",
      // Secondary: lives under Settings → "Other sections" rather than the main
      // rail, on every viewport. This is the section people open least often.
      icon: "ScrollText",
      secondary: true,
      description: "Every change made from this dashboard",
    },
    {
      href: `${base}/control`,
      label: "Bot control",
      // Sits beside the Audit log under Settings — both are operator-facing
      // sections rather than everyday module config. Owner-only, so it's
      // stripped for non-owners below.
      icon: "Cpu",
      secondary: true,
      ownerOnly: true,
      description: "Health, cogs, logs and restart",
    },
    {
      href: `${base}/settings`,
      label: "Settings",
      icon: "Settings",
      sidebar: true,
      primary: true,
      description: "Appearance and your session",
    },
  ];

  return includeOwner ? items : items.filter((item) => !item.ownerOnly);
}

/**
 * Whether a nav item should look active.
 *
 * Prefix matching, except for the overview — `/dashboard/<id>` is a prefix of
 * every other route, so a naive `startsWith` would light up Overview on every
 * page in the dashboard.
 */
export function isActive(href: string, pathname: string, exact = false): boolean {
  if (exact) return pathname === href;
  return pathname === href || pathname.startsWith(`${href}/`);
}

export interface ModuleGroup {
  category: ModuleSchema["category"];
  label: string;
  modules: ModuleSchema[];
}

/** Modules grouped for display, in a fixed category order. */
export function moduleGroups(modules: ModuleSchema[] = MODULES): ModuleGroup[] {
  return CATEGORY_ORDER.map((category) => ({
    category,
    label: CATEGORY_LABELS[category],
    modules: modules.filter((module) => module.category === category),
  })).filter((group) => group.modules.length > 0);
}
