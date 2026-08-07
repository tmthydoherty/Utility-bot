"use client";

import * as React from "react";
import { AnimatePresence, motion } from "motion/react";
import { CircleAlert, CircleCheck, Info, X } from "lucide-react";

import { cn } from "@/lib/utils";
import { SPRING } from "@/lib/motion";

/**
 * Toasts.
 *
 * Positioned at the bottom on mobile (near the thumb, above the tab bar and
 * the home indicator) and at the top-right on desktop. Errors do not
 * auto-dismiss: a failed save that quietly disappears is how someone comes
 * back an hour later believing a setting was applied.
 */

type ToastVariant = "success" | "error" | "info";

interface Toast {
  id: number;
  title: string;
  description?: string;
  variant: ToastVariant;
}

interface ToastContextValue {
  toast: (input: Omit<Toast, "id">) => void;
  success: (title: string, description?: string) => void;
  error: (title: string, description?: string) => void;
}

const ToastContext = React.createContext<ToastContextValue | null>(null);

export function useToast(): ToastContextValue {
  const ctx = React.useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used inside <ToastProvider>");
  return ctx;
}

const AUTO_DISMISS_MS = 4500;

const ICONS: Record<ToastVariant, React.ElementType> = {
  success: CircleCheck,
  error: CircleAlert,
  info: Info,
};

const ACCENTS: Record<ToastVariant, string> = {
  success: "text-[var(--success)]",
  error: "text-[var(--danger)]",
  info: "text-[var(--accent)]",
};

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = React.useState<Toast[]>([]);
  const nextId = React.useRef(0);

  const dismiss = React.useCallback((id: number) => {
    setToasts((current) => current.filter((t) => t.id !== id));
  }, []);

  const toast = React.useCallback(
    (input: Omit<Toast, "id">) => {
      const id = nextId.current++;
      // Cap the stack. Beyond three, the oldest is unreadable anyway and the
      // column starts covering the content it is describing.
      setToasts((current) => [...current.slice(-2), { ...input, id }]);
      if (input.variant !== "error") {
        setTimeout(() => dismiss(id), AUTO_DISMISS_MS);
      }
    },
    [dismiss],
  );

  const value = React.useMemo<ToastContextValue>(
    () => ({
      toast,
      success: (title, description) => toast({ title, description, variant: "success" }),
      error: (title, description) => toast({ title, description, variant: "error" }),
    }),
    [toast],
  );

  return (
    <ToastContext.Provider value={value}>
      {children}
      <div
        // Announced but never focus-stealing: a toast that grabs focus
        // interrupts whatever the user was typing when it fired.
        role="region"
        aria-label="Notifications"
        className={cn(
          "pointer-events-none fixed z-[100] flex flex-col gap-2",
          "inset-x-3 bottom-[calc(env(safe-area-inset-bottom)+5.5rem)]",
          "sm:inset-x-auto sm:bottom-auto sm:right-4 sm:top-4 sm:w-full sm:max-w-sm",
        )}
      >
        <AnimatePresence initial={false}>
          {toasts.map((item) => {
            const ToastIcon = ICONS[item.variant];
            return (
              <motion.div
                key={item.id}
                layout
                initial={{ opacity: 0, y: 16, scale: 0.96 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, scale: 0.96, transition: { duration: 0.15 } }}
                transition={SPRING}
                role="status"
                aria-live={item.variant === "error" ? "assertive" : "polite"}
                className={cn(
                  "glass glass-highlight pointer-events-auto flex items-start gap-3",
                  "rounded-lg p-3.5 shadow-[var(--elev-4)]",
                )}
              >
                <ToastIcon className={cn("mt-0.5 size-4 shrink-0", ACCENTS[item.variant])} />
                <div className="min-w-0 flex-1 space-y-0.5">
                  <p className="text-sm font-medium leading-snug">{item.title}</p>
                  {item.description && (
                    <p className="text-xs leading-relaxed text-fg-muted">{item.description}</p>
                  )}
                </div>
                <button
                  type="button"
                  onClick={() => dismiss(item.id)}
                  aria-label="Dismiss"
                  className="grid size-6 shrink-0 place-items-center rounded text-fg-subtle transition-colors hover:text-fg"
                >
                  <X className="size-3.5" />
                </button>
              </motion.div>
            );
          })}
        </AnimatePresence>
      </div>
    </ToastContext.Provider>
  );
}
