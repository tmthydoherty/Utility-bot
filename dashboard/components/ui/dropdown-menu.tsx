"use client";

import * as React from "react";
import * as DropdownPrimitive from "@radix-ui/react-dropdown-menu";
import { Check } from "lucide-react";

import { cn } from "@/lib/utils";

export const DropdownMenu = DropdownPrimitive.Root;
export const DropdownMenuTrigger = DropdownPrimitive.Trigger;
export const DropdownMenuGroup = DropdownPrimitive.Group;

export const DropdownMenuContent = React.forwardRef<
  React.ComponentRef<typeof DropdownPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof DropdownPrimitive.Content>
>(({ className, sideOffset = 8, align = "end", ...props }, ref) => (
  <DropdownPrimitive.Portal>
    <DropdownPrimitive.Content
      ref={ref}
      sideOffset={sideOffset}
      align={align}
      className={cn(
        "pop-anim z-50 min-w-[200px] overflow-hidden rounded-lg p-1.5",
        "border border-[var(--border-strong)] bg-[var(--surface-solid)] shadow-[var(--elev-4)]",
        // Solid rather than glass: a translucent menu over dense content is
        // unreadable, and this is the one place clarity beats the aesthetic.
        className,
      )}
      {...props}
    />
  </DropdownPrimitive.Portal>
));
DropdownMenuContent.displayName = "DropdownMenuContent";

const itemClasses = cn(
  "relative flex cursor-pointer select-none items-center gap-2.5 rounded-md px-2.5 py-2",
  "text-sm text-fg outline-none transition-colors",
  "focus:bg-[var(--surface-hover)] data-[highlighted]:bg-[var(--surface-hover)]",
  "data-[disabled]:pointer-events-none data-[disabled]:opacity-50",
  "[&_svg]:size-4 [&_svg]:shrink-0 [&_svg]:text-fg-muted",
);

export const DropdownMenuItem = React.forwardRef<
  React.ComponentRef<typeof DropdownPrimitive.Item>,
  React.ComponentPropsWithoutRef<typeof DropdownPrimitive.Item> & { destructive?: boolean }
>(({ className, destructive, ...props }, ref) => (
  <DropdownPrimitive.Item
    ref={ref}
    className={cn(
      itemClasses,
      destructive && "text-[var(--danger)] [&_svg]:text-[var(--danger)]",
      className,
    )}
    {...props}
  />
));
DropdownMenuItem.displayName = "DropdownMenuItem";

export const DropdownMenuRadioGroup = DropdownPrimitive.RadioGroup;

export const DropdownMenuRadioItem = React.forwardRef<
  React.ComponentRef<typeof DropdownPrimitive.RadioItem>,
  React.ComponentPropsWithoutRef<typeof DropdownPrimitive.RadioItem>
>(({ className, children, ...props }, ref) => (
  <DropdownPrimitive.RadioItem ref={ref} className={cn(itemClasses, "pr-8", className)} {...props}>
    {children}
    <DropdownPrimitive.ItemIndicator className="absolute right-2.5">
      <Check className="size-4 text-[var(--accent)]" />
    </DropdownPrimitive.ItemIndicator>
  </DropdownPrimitive.RadioItem>
));
DropdownMenuRadioItem.displayName = "DropdownMenuRadioItem";

export function DropdownMenuSeparator({ className }: { className?: string }) {
  return <DropdownPrimitive.Separator className={cn("my-1.5 h-px bg-[var(--border)]", className)} />;
}

export function DropdownMenuLabel({
  className,
  ...props
}: React.ComponentPropsWithoutRef<typeof DropdownPrimitive.Label>) {
  return (
    <DropdownPrimitive.Label
      className={cn("px-2.5 py-1.5 text-xs font-medium uppercase tracking-wide text-fg-subtle", className)}
      {...props}
    />
  );
}
