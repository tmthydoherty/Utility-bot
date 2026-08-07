"use client";

import * as React from "react";
import * as DialogPrimitive from "@radix-ui/react-dialog";
import { AnimatePresence, motion, type PanInfo } from "motion/react";
import { X } from "lucide-react";

import { cn } from "@/lib/utils";
import { SPRING_SOFT, transitions } from "@/lib/motion";
import { useIsMobile } from "@/lib/hooks/use-media-query";

/**
 * One overlay component, two presentations.
 *
 * On a phone this is a bottom sheet you can throw off the bottom of the
 * screen; on a desktop it is a centred dialog. They are the same component
 * because they hold the same content, and maintaining two would guarantee
 * they drift apart.
 *
 * Radix supplies the hard parts — focus trap, scroll lock, aria wiring, Escape
 * — and the animation library supplies presence and the drag. It is controlled
 * rather than self-managing so `open` has exactly one owner; a sheet with its
 * own private copy of that state is how you end up with an invisible dialog
 * still trapping focus.
 */

const DISMISS_DISTANCE = 110;
const DISMISS_VELOCITY = 420;

const SIZES = {
  sm: "sm:max-w-md",
  md: "sm:max-w-lg",
  lg: "sm:max-w-2xl",
} as const;

export interface SheetProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Accessible name. Rendered visibly unless `hideTitle`. */
  title: string;
  description?: string;
  hideTitle?: boolean;
  size?: keyof typeof SIZES;
  footer?: React.ReactNode;
  className?: string;
  children: React.ReactNode;
}

export function Sheet({
  open,
  onOpenChange,
  title,
  description,
  hideTitle,
  size = "md",
  footer,
  className,
  children,
}: SheetProps) {
  const isMobile = useIsMobile();

  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Portal forceMount>
        <AnimatePresence>
          {open && (
            <>
              <DialogPrimitive.Overlay asChild forceMount>
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={transitions.standard}
                  className="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm"
                />
              </DialogPrimitive.Overlay>

              <DialogPrimitive.Content asChild forceMount>
                <motion.div
                  className={cn(
                    "fixed inset-x-0 bottom-0 z-50 flex max-h-[92dvh] flex-col",
                    "glass glass-highlight rounded-t-xl shadow-[var(--elev-4)]",
                    // Desktop: lift off the bottom edge and centre it.
                    "sm:inset-auto sm:left-1/2 sm:top-1/2 sm:w-full sm:rounded-xl",
                    "sm:max-h-[85vh]",
                    SIZES[size],
                    className,
                  )}
                  // Drag is mobile-only: on desktop the sheet is positioned
                  // with a centring transform that a drag would fight.
                  drag={isMobile ? "y" : false}
                  dragDirectionLock
                  dragConstraints={{ top: 0, bottom: 0 }}
                  dragElastic={{ top: 0, bottom: 0.6 }}
                  onDragEnd={(_, info: PanInfo) => {
                    if (info.offset.y > DISMISS_DISTANCE || info.velocity.y > DISMISS_VELOCITY) {
                      onOpenChange(false);
                    }
                  }}
                  initial={isMobile ? { y: "100%" } : { opacity: 0, scale: 0.97, x: "-50%", y: "-50%" }}
                  animate={isMobile ? { y: 0 } : { opacity: 1, scale: 1, x: "-50%", y: "-50%" }}
                  exit={isMobile ? { y: "100%" } : { opacity: 0, scale: 0.98, x: "-50%", y: "-50%" }}
                  transition={SPRING_SOFT}
                  style={{ paddingBottom: "env(safe-area-inset-bottom)" }}
                >
                  {/* Grab handle: the only affordance that says this can be
                      thrown away, so it belongs only where dragging works. */}
                  <div className="flex shrink-0 justify-center pt-3 sm:hidden">
                    <div className="h-1 w-10 rounded-full bg-[var(--border-strong)]" />
                  </div>

                  <div className="flex shrink-0 items-start justify-between gap-4 px-5 pb-3 pt-4 sm:px-6 sm:pt-6">
                    <div className="min-w-0 space-y-1">
                      <DialogPrimitive.Title
                        className={cn("text-base font-semibold tracking-tight", hideTitle && "sr-only")}
                      >
                        {title}
                      </DialogPrimitive.Title>
                      {/* Always present: Radix warns without one, and a dialog
                          with no description is a genuine screen-reader gap. */}
                      <DialogPrimitive.Description
                        className={cn(
                          "text-sm leading-relaxed text-fg-muted",
                          !description && "sr-only",
                        )}
                      >
                        {description ?? title}
                      </DialogPrimitive.Description>
                    </div>
                    <DialogPrimitive.Close
                      className={cn(
                        "grid size-9 shrink-0 place-items-center rounded-md text-fg-muted",
                        "transition-colors hover:bg-[var(--surface-hover)] hover:text-fg",
                      )}
                      aria-label="Close"
                    >
                      <X className="size-4" />
                    </DialogPrimitive.Close>
                  </div>

                  <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-5 pb-5 sm:px-6 sm:pb-6">
                    {children}
                  </div>

                  {footer && (
                    <div className="shrink-0 border-t border-[var(--border)] px-5 py-4 sm:px-6">
                      {footer}
                    </div>
                  )}
                </motion.div>
              </DialogPrimitive.Content>
            </>
          )}
        </AnimatePresence>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}
