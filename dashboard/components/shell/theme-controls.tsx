"use client";

import { Monitor, Moon, Sun } from "lucide-react";

import { cn } from "@/lib/utils";
import {
  useAppearance,
  type Density,
  type MotionPreference,
  type ThemePreference,
} from "@/components/providers/theme-provider";

/**
 * Appearance controls, shared by the mobile "More" sheet and the settings page.
 *
 * Motion is a first-class preference rather than something buried, because
 * this design leans hard on movement and some people need it to stop. The
 * control only ever *adds* restriction — someone whose OS already asks for
 * reduced motion gets it regardless of what is selected here.
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
  const { theme, setTheme, motion, setMotion, density, setDensity } = useAppearance();

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
      <SegmentedControl<MotionPreference>
        label="Motion"
        value={motion}
        onChange={setMotion}
        options={[
          { value: "full", label: "Full" },
          { value: "reduced", label: "Reduced" },
        ]}
      />
      <SegmentedControl<Density>
        label="Density"
        value={density}
        onChange={setDensity}
        options={[
          { value: "comfortable", label: "Comfortable" },
          { value: "compact", label: "Compact" },
        ]}
      />
    </div>
  );
}
