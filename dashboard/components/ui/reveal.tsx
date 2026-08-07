"use client";

import { motion } from "motion/react";

import { listItemVariants } from "@/lib/motion";

/**
 * Entrance animation for a block of content.
 *
 * `whileInView` with `once` so a long page animates as you reach it rather
 * than all at once above the fold — and never re-animates on scroll back up,
 * which reads as a glitch.
 *
 * The stagger delay is capped inside `listItemVariants`, so passing a large
 * index is safe.
 */
export function Reveal({
  children,
  delay = 0,
  className,
}: {
  children: React.ReactNode;
  /** Stagger position, not milliseconds. */
  delay?: number;
  className?: string;
}) {
  return (
    <motion.div
      className={className}
      custom={delay}
      variants={listItemVariants}
      initial="hidden"
      whileInView="visible"
      viewport={{ once: true, margin: "-60px" }}
    >
      {children}
    </motion.div>
  );
}
