import type { DiscordGuild } from "./types";

/**
 * Formatting helpers that are safe on both sides of the server/client
 * boundary — no bot token, no `server-only`.
 */

export function guildIconUrl(
  guild: Pick<DiscordGuild, "id" | "icon">,
  size = 128,
): string | null {
  if (!guild.icon) return null;
  const ext = guild.icon.startsWith("a_") ? "gif" : "png";
  return `https://cdn.discordapp.com/icons/${guild.id}/${guild.icon}.${ext}?size=${size}`;
}

/**
 * Discord treats colour 0 as "no colour" — the role inherits — rather than as
 * black. Rendering it literally makes half a role list look like a rendering
 * bug on a dark theme.
 */
export function roleColorToCssServer(color: number): string | null {
  if (!color) return null;
  return `#${color.toString(16).padStart(6, "0")}`;
}
