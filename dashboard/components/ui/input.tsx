"use client";

import * as React from "react";

import { cn } from "@/lib/utils";

const fieldBase = cn(
  "w-full rounded-md border border-[var(--border-strong)] bg-[var(--surface)]",
  "px-3 text-fg placeholder:text-fg-subtle",
  "transition-[border-color,box-shadow,background-color] duration-150 ease-out",
  "hover:border-[var(--fg-subtle)]",
  "focus:border-[var(--accent)] focus:bg-[var(--surface-hover)]",
  "focus:outline-none focus:ring-4 focus:ring-[var(--accent-soft)]",
  "disabled:cursor-not-allowed disabled:opacity-50",
  "aria-[invalid=true]:border-[var(--danger)] aria-[invalid=true]:ring-[var(--danger-soft)]",
);

export const Input = React.forwardRef<HTMLInputElement, React.ComponentProps<"input">>(
  ({ className, ...props }, ref) => (
    <input
      ref={ref}
      // 16px on mobile is not a style choice: iOS Safari zooms the viewport on
      // focus for anything smaller, and the page never zooms back out.
      className={cn(fieldBase, "h-11 text-base sm:text-sm", className)}
      {...props}
    />
  ),
);
Input.displayName = "Input";

export const Textarea = React.forwardRef<HTMLTextAreaElement, React.ComponentProps<"textarea">>(
  ({ className, ...props }, ref) => (
    <textarea
      ref={ref}
      className={cn(fieldBase, "min-h-[104px] resize-y py-2.5 text-base leading-relaxed sm:text-sm", className)}
      {...props}
    />
  ),
);
Textarea.displayName = "Textarea";

export function Label({
  className,
  required,
  children,
  ...props
}: React.ComponentProps<"label"> & { required?: boolean }) {
  return (
    <label className={cn("text-sm font-medium text-fg", className)} {...props}>
      {children}
      {required && (
        <span className="ml-1 text-[var(--danger)]" aria-hidden>
          *
        </span>
      )}
    </label>
  );
}

export function FieldHint({ className, ...props }: React.ComponentProps<"p">) {
  return <p className={cn("text-xs leading-relaxed text-fg-muted", className)} {...props} />;
}

export function FieldError({ className, children, ...props }: React.ComponentProps<"p">) {
  if (!children) return null;
  return (
    <p
      role="alert"
      className={cn("text-xs font-medium text-[var(--danger)]", className)}
      {...props}
    >
      {children}
    </p>
  );
}
