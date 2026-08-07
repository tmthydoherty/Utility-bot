"use client";

import { motion } from "motion/react";

import { pageVariants } from "@/lib/motion";

/**
 * Page transition.
 *
 * A template rather than a layout because Next remounts a template on every
 * navigation, which is what makes the enter animation run again — a layout
 * would animate once and then sit still for the rest of the session.
 *
 * Eight pixels and 400ms. Anything more and navigation starts to feel like
 * waiting rather than moving.
 */
export default function DashboardTemplate({ children }: { children: React.ReactNode }) {
  return (
    <motion.div variants={pageVariants} initial="hidden" animate="visible">
      {children}
    </motion.div>
  );
}
