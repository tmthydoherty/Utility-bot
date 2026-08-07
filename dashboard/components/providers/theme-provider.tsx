"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import { MotionConfig } from "motion/react";

export type ThemePreference = "dark" | "light" | "system";
export type MotionPreference = "full" | "reduced";
export type Density = "comfortable" | "compact";

export const STORAGE_KEYS = {
  theme: "vibey.theme",
  motion: "vibey.motion",
  density: "vibey.density",
} as const;

type Appearance = {
  theme: ThemePreference;
  /** What is actually painted right now — `system` resolved against the OS. */
  resolvedTheme: "dark" | "light";
  motion: MotionPreference;
  density: Density;
  setTheme: (value: ThemePreference) => void;
  setMotion: (value: MotionPreference) => void;
  setDensity: (value: Density) => void;
};

const AppearanceContext = createContext<Appearance | null>(null);

export function useAppearance(): Appearance {
  const ctx = useContext(AppearanceContext);
  if (!ctx) throw new Error("useAppearance must be used inside <ThemeProvider>");
  return ctx;
}

/**
 * Runs before first paint, inlined in <head>.
 *
 * React cannot help here: by the time hydration runs the browser has already
 * painted, and a white flash on a near-black dashboard is jarring enough that
 * people notice it every single navigation. Kept as a string so it ships as
 * one statement with a CSP nonce rather than as a bundle.
 */
export const themeInitScript = `
(function(){try{
var d=document.documentElement;
var t=localStorage.getItem('${STORAGE_KEYS.theme}')||'dark';
var resolved=t==='system'?(matchMedia('(prefers-color-scheme: light)').matches?'light':'dark'):t;
d.setAttribute('data-theme',resolved);
d.setAttribute('data-motion',localStorage.getItem('${STORAGE_KEYS.motion}')||'full');
d.setAttribute('data-density',localStorage.getItem('${STORAGE_KEYS.density}')||'comfortable');
}catch(e){document.documentElement.setAttribute('data-theme','dark');}})();
`.trim();

function readStored<T extends string>(key: string, fallback: T): T {
  if (typeof window === "undefined") return fallback;
  try {
    return (localStorage.getItem(key) as T | null) ?? fallback;
  } catch {
    // Private-mode Safari throws on localStorage access.
    return fallback;
  }
}

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  // Initialised from storage rather than from a constant, so the value the
  // pre-paint script picked is the value React continues with — otherwise the
  // first client render would fight the script and flip the theme back.
  const [theme, setThemeState] = useState<ThemePreference>(() =>
    readStored<ThemePreference>(STORAGE_KEYS.theme, "dark"),
  );
  const [motionPref, setMotionState] = useState<MotionPreference>(() =>
    readStored<MotionPreference>(STORAGE_KEYS.motion, "full"),
  );
  const [density, setDensityState] = useState<Density>(() =>
    readStored<Density>(STORAGE_KEYS.density, "comfortable"),
  );
  const [systemTheme, setSystemTheme] = useState<"dark" | "light">("dark");

  useEffect(() => {
    const query = window.matchMedia("(prefers-color-scheme: light)");
    const sync = () => setSystemTheme(query.matches ? "light" : "dark");
    sync();
    query.addEventListener("change", sync);
    return () => query.removeEventListener("change", sync);
  }, []);

  const resolvedTheme = theme === "system" ? systemTheme : theme;

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", resolvedTheme);
    // Keeps the mobile browser chrome (address bar / status bar) matching the
    // canvas, which is most of what makes an installed PWA feel native.
    const meta = document.querySelector('meta[name="theme-color"]');
    meta?.setAttribute("content", resolvedTheme === "light" ? "#f6f6fa" : "#0a0a0f");
  }, [resolvedTheme]);

  useEffect(() => {
    document.documentElement.setAttribute("data-motion", motionPref);
  }, [motionPref]);

  useEffect(() => {
    document.documentElement.setAttribute("data-density", density);
  }, [density]);

  // A backgrounded tab still composites CSS animations, so the aurora would
  // keep the GPU busy behind another window forever. Park it.
  useEffect(() => {
    const onVisibility = () => {
      document.documentElement.setAttribute(
        "data-tab-hidden",
        document.hidden ? "true" : "false",
      );
    };
    onVisibility();
    document.addEventListener("visibilitychange", onVisibility);
    return () => document.removeEventListener("visibilitychange", onVisibility);
  }, []);

  const persist = useCallback((key: string, value: string) => {
    try {
      localStorage.setItem(key, value);
    } catch {
      // Preference is still applied for this session; it just won't survive.
    }
  }, []);

  const value = useMemo<Appearance>(
    () => ({
      theme,
      resolvedTheme,
      motion: motionPref,
      density,
      setTheme: (next) => {
        setThemeState(next);
        persist(STORAGE_KEYS.theme, next);
      },
      setMotion: (next) => {
        setMotionState(next);
        persist(STORAGE_KEYS.motion, next);
      },
      setDensity: (next) => {
        setDensityState(next);
        persist(STORAGE_KEYS.density, next);
      },
    }),
    [theme, resolvedTheme, motionPref, density, persist],
  );

  return (
    <AppearanceContext.Provider value={value}>
      {/* "user" defers to the OS setting; the in-app toggle can only ever be
          more restrictive, never less. */}
      <MotionConfig reducedMotion={motionPref === "reduced" ? "always" : "user"}>
        {children}
      </MotionConfig>
    </AppearanceContext.Provider>
  );
}
