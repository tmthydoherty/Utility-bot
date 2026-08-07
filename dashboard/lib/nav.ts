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
  /** Shown in the mobile tab bar. Anything else lives behind "More". */
  primary?: boolean;
  description?: string;
}

export function navItems(guildId: string): NavItem[] {
  const base = `/dashboard/${guildId}`;
  return [
    {
      href: base,
      label: "Overview",
      icon: "LayoutDashboard",
      primary: true,
      description: "Server health at a glance",
    },
    {
      href: `${base}/modules`,
      label: "Modules",
      icon: "Blocks",
      primary: true,
      description: "Turn features on and configure them",
    },
    {
      href: `${base}/audit`,
      label: "Audit log",
      icon: "ScrollText",
      primary: true,
      description: "Every change made from this dashboard",
    },
    {
      href: `${base}/settings`,
      label: "Settings",
      icon: "Settings",
      primary: true,
      description: "Appearance, motion and your session",
    },
  ];
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
