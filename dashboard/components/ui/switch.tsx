"use client";

import * as React from "react";
import * as SwitchPrimitive from "@radix-ui/react-switch";

import { cn } from "@/lib/utils";

export const Switch = React.forwardRef<
  React.ComponentRef<typeof SwitchPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof SwitchPrimitive.Root>
>(({ className, ...props }, ref) => (
  <SwitchPrimitive.Root
    ref={ref}
    className={cn(
      "peer group relative inline-flex h-6 w-11 shrink-0 cursor-pointer items-center rounded-full",
      "border border-[var(--border-strong)] transition-colors duration-200 ease-out",
      "data-[state=unchecked]:bg-[var(--surface-hover)]",
      "data-[state=checked]:border-transparent data-[state=checked]:shadow-[var(--glow)]",
      "disabled:cursor-not-allowed disabled:opacity-50",
      className,
    )}
    {...props}
  >
    {/* The gradient lives on its own layer and fades in, because animating a
        gradient background between two states does not interpolate. */}
    <span
      aria-hidden
      className={cn(
        "accent-gradient pointer-events-none absolute inset-0 rounded-full opacity-0 transition-opacity duration-200",
        "group-data-[state=checked]:opacity-100",
      )}
    />
    <SwitchPrimitive.Thumb
      className={cn(
        "pointer-events-none relative z-10 block size-[18px] rounded-full bg-white shadow-sm",
        "transition-transform duration-200 ease-[cubic-bezier(0.22,1,0.36,1)]",
        "translate-x-[3px] data-[state=checked]:translate-x-[23px]",
      )}
    />
  </SwitchPrimitive.Root>
));
Switch.displayName = "Switch";
