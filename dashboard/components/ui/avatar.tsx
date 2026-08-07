"use client";

import * as React from "react";
import * as AvatarPrimitive from "@radix-ui/react-avatar";

import { cn, initials } from "@/lib/utils";

const SIZES = {
  sm: "size-7 text-[10px]",
  md: "size-9 text-xs",
  lg: "size-12 text-sm",
  xl: "size-16 text-lg",
} as const;

export function Avatar({
  src,
  name,
  size = "md",
  className,
  rounded = "full",
}: {
  src?: string | null;
  name: string;
  size?: keyof typeof SIZES;
  className?: string;
  /** Guild icons are squircles in Discord; users are circles. */
  rounded?: "full" | "md";
}) {
  return (
    <AvatarPrimitive.Root
      className={cn(
        "relative grid shrink-0 place-items-center overflow-hidden",
        "border border-[var(--border)] bg-[var(--surface-hover)]",
        rounded === "full" ? "rounded-full" : "rounded-lg",
        SIZES[size],
        className,
      )}
    >
      {src && (
        <AvatarPrimitive.Image
          src={src}
          alt=""
          className="size-full object-cover"
          // Radix only swaps in the fallback once the image has actually
          // failed, so a slow CDN shows initials rather than an empty hole.
          loading="lazy"
        />
      )}
      <AvatarPrimitive.Fallback
        delayMs={src ? 300 : 0}
        className="grid size-full place-items-center font-semibold text-fg-muted"
      >
        {initials(name)}
      </AvatarPrimitive.Fallback>
    </AvatarPrimitive.Root>
  );
}
