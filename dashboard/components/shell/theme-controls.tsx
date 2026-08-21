"use client";

import { Monitor, Moon, Sun } from "lucide-react";

import { cn } from "@/lib/utils";
import {
  useAppearance,
  type ThemePreference,
} from "@/components/providers/theme-provider";

/**
 * Appearance controls, shared by the mobile "More" sheet and the settings page.
 *
 * Just the theme now. Motion is left to the OS `prefers-reduced-motion` setting
 * (honoured globally in globals.css and by MotionConfig), which is where people
 * who need less movement already express it — a second in-app toggle for it, and
 * a density toggle, were more surface than they earned.
 */

function SegmentedControl<T extends string>({
  label,
  value,
  onChange,
  options,
}: {
  label: string;
  value: T;
  onChange: (value: T) => void;
  options: { value: T; label: string; icon?: React.ElementType }[];
}) {
  return (
    <div className="space-y-2">
      <p className="text-sm font-medium">{label}</p>
      <div
        role="radiogroup"
        aria-label={label}
        className="glass grid grid-cols-3 gap-1 rounded-lg p-1"
        style={{ gridTemplateColumns: `repeat(${options.length}, minmax(0, 1fr))` }}
      >
        {options.map((option) => {
          const selected = option.value === value;
          const OptionIcon = option.icon;
          return (
            <button
              key={option.value}
              type="button"
              role="radio"
              aria-checked={selected}
              onClick={() => onChange(option.value)}
              className={cn(
                "flex h-10 items-center justify-center gap-2 rounded-md text-sm font-medium",
                "transition-colors duration-150",
                selected
                  ? "bg-[var(--surface-active)] text-fg shadow-[var(--elev-1)]"
                  : "text-fg-muted hover:text-fg",
              )}
            >
              {OptionIcon && <OptionIcon className="size-4" aria-hidden />}
              {option.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}

export function ThemeControls() {
  const { theme, setTheme } = useAppearance();

  return (
    <div className="space-y-5">
      <SegmentedControl<ThemePreference>
        label="Theme"
        value={theme}
        onChange={setTheme}
        options={[
          { value: "dark", label: "Dark", icon: Moon },
          { value: "light", label: "Light", icon: Sun },
          { value: "system", label: "Auto", icon: Monitor },
        ]}
      />
    </div>
  );
}
