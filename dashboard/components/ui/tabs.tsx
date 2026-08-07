"use client";

import * as React from "react";
import * as TabsPrimitive from "@radix-ui/react-tabs";
import { motion } from "motion/react";

import { cn } from "@/lib/utils";
import { SPRING } from "@/lib/motion";

export const Tabs = TabsPrimitive.Root;

export const TabsList = React.forwardRef<
  React.ComponentRef<typeof TabsPrimitive.List>,
  React.ComponentPropsWithoutRef<typeof TabsPrimitive.List>
>(({ className, ...props }, ref) => (
  <TabsPrimitive.List
    ref={ref}
    className={cn(
      "glass inline-flex h-11 items-center gap-1 rounded-lg p-1",
      // Tab strips overflow constantly on a phone; scroll them rather than
      // wrapping, which would change the header's height as tabs are added.
      "max-w-full overflow-x-auto [scrollbar-width:none] [&::-webkit-scrollbar]:hidden",
      className,
    )}
    {...props}
  />
));
TabsList.displayName = "TabsList";

/**
 * A tab whose active background is a shared element.
 *
 * The pill is one element that moves between tabs via `layoutId` rather than a
 * background that fades in and out per tab — the movement is what tells the
 * eye where the selection went.
 */
export const TabsTrigger = React.forwardRef<
  React.ComponentRef<typeof TabsPrimitive.Trigger>,
  React.ComponentPropsWithoutRef<typeof TabsPrimitive.Trigger> & { layoutGroup?: string }
>(({ className, children, layoutGroup = "tabs", value, ...props }, ref) => (
  <TabsPrimitive.Trigger
    ref={ref}
    value={value}
    className={cn(
      "group relative inline-flex h-9 shrink-0 items-center justify-center gap-2 rounded-md px-3.5",
      "text-sm font-medium text-fg-muted outline-none transition-colors",
      "hover:text-fg data-[state=active]:text-fg",
      "[&_svg]:size-4",
      className,
    )}
    {...props}
  >
    <span className="relative z-10 flex items-center gap-2">{children}</span>
    <span className="absolute inset-0 hidden group-data-[state=active]:block">
      <motion.span
        layoutId={`${layoutGroup}-pill`}
        transition={SPRING}
        className="absolute inset-0 rounded-md bg-[var(--surface-active)] shadow-[var(--elev-1)]"
      />
    </span>
  </TabsPrimitive.Trigger>
));
TabsTrigger.displayName = "TabsTrigger";

export const TabsContent = React.forwardRef<
  React.ComponentRef<typeof TabsPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof TabsPrimitive.Content>
>(({ className, ...props }, ref) => (
  <TabsPrimitive.Content ref={ref} className={cn("mt-5 outline-none", className)} {...props} />
));
TabsContent.displayName = "TabsContent";
