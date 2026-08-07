import { redirect } from "next/navigation";

import { primaryGuildId } from "@/lib/guild";

/**
 * The dashboard's entry point.
 *
 * The bot is in four servers but only one has any activity, and the session is
 * only ever granted against that one — so a server picker would be a screen
 * with a single card on it, shown before every visit. It redirects instead.
 * When a second real server appears, this is where the picker goes.
 */
export default function DashboardIndex() {
  redirect(`/dashboard/${primaryGuildId()}`);
}
