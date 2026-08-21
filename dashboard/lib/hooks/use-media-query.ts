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

/**
 * Height, in pixels, of the on-screen keyboard (or any bottom-docked virtual
 * widget) currently covering the layout viewport — 0 when nothing is.
 *
 * Read from the VisualViewport API, which both iOS Safari (keyboard overlays,
 * `offsetTop` moves) and Android Chrome (viewport shrinks) report through, so
 * the difference from the layout viewport is the covered strip either way. A
 * bottom sheet lifts itself by this amount so its search results don't end up
 * behind the keyboard the search box just summoned.
 */
export function useKeyboardInset(): number {
  const [inset, setInset] = useState(0);

  useEffect(() => {
    const vv = window.visualViewport;
    if (!vv) return;
    const update = () => {
      const covered = window.innerHeight - vv.height - vv.offsetTop;
      setInset(Math.max(0, Math.round(covered)));
    };
    update();
    vv.addEventListener("resize", update);
    vv.addEventListener("scroll", update);
    return () => {
      vv.removeEventListener("resize", update);
      vv.removeEventListener("scroll", update);
    };
  }, []);

  return inset;
}
