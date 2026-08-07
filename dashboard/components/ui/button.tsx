"use client";

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import { Loader2 } from "lucide-react";

import { cn } from "@/lib/utils";

const buttonVariants = cva(
  cn(
    "relative inline-flex items-center justify-center gap-2 whitespace-nowrap font-medium",
    "select-none rounded-md transition-all duration-150 ease-out",
    // Every interactive element gets a real press response. On touch this is
    // most of what makes the UI feel like an app rather than a web page.
    "active:scale-[0.97]",
    "disabled:pointer-events-none disabled:opacity-50",
    "[&_svg]:shrink-0",
  ),
  {
    variants: {
      variant: {
        primary: cn(
          "accent-gradient text-[var(--accent-fg)] shadow-[var(--elev-2)]",
          "hover:shadow-[var(--glow)] hover:brightness-110",
        ),
        secondary: cn(
          "glass text-fg",
          "hover:bg-[var(--surface-hover)] hover:border-[var(--border-strong)]",
        ),
        ghost: "text-fg-muted hover:bg-[var(--surface)] hover:text-fg",
        outline: cn(
          "border border-[var(--border-strong)] bg-transparent text-fg",
          "hover:bg-[var(--surface)]",
        ),
        danger: cn(
          "bg-[var(--danger-soft)] text-[var(--danger)] border border-transparent",
          "hover:border-[var(--danger)]",
        ),
        link: "text-accent underline-offset-4 hover:underline active:scale-100",
      },
      size: {
        // 44px: the minimum comfortable touch target, and the reason the
        // default is not the 36px that looks tidier on a desktop mockup.
        default: "h-11 px-4 text-sm [&_svg]:size-4",
        sm: "h-9 px-3 text-sm [&_svg]:size-4",
        lg: "h-12 px-6 text-base [&_svg]:size-5",
        icon: "size-11 [&_svg]:size-[18px]",
        "icon-sm": "size-9 [&_svg]:size-4",
      },
    },
    defaultVariants: { variant: "secondary", size: "default" },
  },
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
  loading?: boolean;
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, asChild, loading, children, disabled, ...props }, ref) => {
    const Comp = asChild ? Slot : "button";
    return (
      <Comp
        ref={ref}
        className={cn(buttonVariants({ variant, size }), className)}
        disabled={disabled || loading}
        aria-busy={loading || undefined}
        {...props}
      >
        {loading ? (
          <>
            <Loader2 className="animate-spin" aria-hidden />
            {/* The label stays mounted so the button keeps its width and the
                layout around it doesn't jump on every submit. */}
            <span className="opacity-70">{children}</span>
          </>
        ) : (
          children
        )}
      </Comp>
    );
  },
);
Button.displayName = "Button";

export { buttonVariants };
