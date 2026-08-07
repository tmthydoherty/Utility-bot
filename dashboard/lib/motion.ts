import type { Transition, Variants } from "motion/react";

/**
 * One motion vocabulary for the whole app.
 *
 * Components import from here rather than writing durations inline. That is
 * what keeps a hundred small animations feeling like one system instead of a
 * hundred opinions, and it means retuning the app's feel is editing this file.
 *
 * Reduced motion is handled globally in globals.css (which zeroes every
 * transition) and by MotionConfig in ThemeProvider, so nothing here needs a
 * conditional.
 */

export const DURATION = {
  /** Hover, press, colour — must feel instant. */
  micro: 0.15,
  /** The default: enter/exit of anything already on screen. */
  standard: 0.25,
  /** First paint of a page or a panel sliding in. */
  entrance: 0.4,
} as const;

export const EASE = {
  /** Decelerating. Correct for anything entering. */
  out: [0.22, 1, 0.36, 1],
  /** Symmetric. Correct for anything moving between two states. */
  inOut: [0.83, 0, 0.17, 1],
} as const;

/** Layout movement reads as physical, so springs rather than durations. */
export const SPRING: Transition = {
  type: "spring",
  stiffness: 380,
  damping: 32,
  mass: 0.9,
};

/** Softer spring for large surfaces (sheets, drawers) where snap feels cheap. */
export const SPRING_SOFT: Transition = {
  type: "spring",
  stiffness: 260,
  damping: 30,
  mass: 1,
};

export const transitions = {
  micro: { duration: DURATION.micro, ease: EASE.out } satisfies Transition,
  standard: { duration: DURATION.standard, ease: EASE.out } satisfies Transition,
  entrance: { duration: DURATION.entrance, ease: EASE.out } satisfies Transition,
};

/** Page-level: an 8px rise. Any further and navigation starts to feel slow. */
export const pageVariants: Variants = {
  hidden: { opacity: 0, y: 8 },
  visible: { opacity: 1, y: 0, transition: transitions.entrance },
  exit: { opacity: 0, y: -4, transition: transitions.micro },
};

export const fadeVariants: Variants = {
  hidden: { opacity: 0 },
  visible: { opacity: 1, transition: transitions.standard },
  exit: { opacity: 0, transition: transitions.micro },
};

export const scaleInVariants: Variants = {
  hidden: { opacity: 0, scale: 0.96, y: 4 },
  visible: { opacity: 1, scale: 1, y: 0, transition: SPRING },
  exit: { opacity: 0, scale: 0.98, transition: transitions.micro },
};

/**
 * Stagger, capped.
 *
 * 30ms per item feels alive on a short list and interminable on a long one —
 * a 30-module grid at 30ms is a near-second of waiting before the last card
 * lands. The cap means item 10 and item 300 start at the same time.
 */
const STAGGER_STEP = 0.03;
const STAGGER_CAP = 10;

export const listVariants: Variants = {
  hidden: {},
  visible: { transition: { staggerChildren: STAGGER_STEP } },
};

export const listItemVariants: Variants = {
  hidden: { opacity: 0, y: 12 },
  visible: (index: number = 0) => ({
    opacity: 1,
    y: 0,
    transition: {
      ...transitions.standard,
      delay: Math.min(index, STAGGER_CAP) * STAGGER_STEP,
    },
  }),
};

/** Bottom sheet / mobile drawer. */
export const sheetVariants: Variants = {
  hidden: { y: "100%" },
  visible: { y: 0, transition: SPRING_SOFT },
  exit: { y: "100%", transition: { duration: DURATION.standard, ease: EASE.inOut } },
};

/** Shared `layoutId` for the sidebar/tab active indicator. */
export const ACTIVE_PILL_ID = "nav-active-pill";
