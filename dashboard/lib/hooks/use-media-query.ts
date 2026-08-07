"use client";

import { useEffect, useState } from "react";

/**
 * Media query as state.
 *
 * Starts false on the server and on the first client render, then corrects in
 * an effect. Anything that must be right in the initial HTML should use a CSS
 * breakpoint instead — this is for behaviour (whether a sheet can be dragged,
 * whether a keyboard shortcut is worth binding), not for layout.
 */
export function useMediaQuery(query: string): boolean {
  const [matches, setMatches] = useState(false);

  useEffect(() => {
    const list = window.matchMedia(query);
    const sync = () => setMatches(list.matches);
    sync();
    list.addEventListener("change", sync);
    return () => list.removeEventListener("change", sync);
  }, [query]);

  return matches;
}

/** Matches the `sm` breakpoint the layout uses to switch presentation. */
export function useIsMobile(): boolean {
  return useMediaQuery("(max-width: 639px)");
}
