import {
  Activity,
  AlarmClock,
  ArrowRight,
  ArrowUpRight,
  Ban,
  Bell,
  BellRing,
  Blocks,
  Brain,
  ChartLine,
  Clapperboard,
  Coins,
  DoorOpen,
  FileText,
  Gavel,
  Hand,
  Hash,
  Image as ImageIcon,
  LayoutDashboard,
  Lightbulb,
  Link2,
  Lock,
  Mail,
  Map as MapIcon,
  Megaphone,
  MessageCircleQuestion,
  MessagesSquare,
  Mic,
  Music,
  Palette,
  Puzzle,
  Quote,
  Radio,
  ScrollText,
  Send,
  Settings,
  ShieldAlert,
  ShieldCheck,
  SlidersHorizontal,
  Smile,
  Sparkles,
  Swords,
  TicketCheck,
  Trophy,
  UserCog,
  UserMinus,
  Users,
  Volume2,
  Vote,
  Wand2,
  type LucideIcon,
} from "lucide-react";

/**
 * Explicit icon registry.
 *
 * Module schemas name their icon as a string, which normally means either a
 * dynamic import per icon or `import * as Icons`, and the latter pulls the
 * whole ~1500-icon library into the bundle. Naming them here keeps the imports
 * static and tree-shakeable, and an unknown name degrades to a neutral glyph
 * rather than crashing a page.
 */
const REGISTRY: Record<string, LucideIcon> = {
  Activity,
  AlarmClock,
  ArrowRight,
  ArrowUpRight,
  Ban,
  Bell,
  BellRing,
  Blocks,
  Brain,
  ChartLine,
  Clapperboard,
  Coins,
  DoorOpen,
  FileText,
  Gavel,
  Hand,
  Hash,
  Image: ImageIcon,
  LayoutDashboard,
  Lightbulb,
  Link2,
  Lock,
  Mail,
  Map: MapIcon,
  Megaphone,
  MessageCircleQuestion,
  MessagesSquare,
  Mic,
  Music,
  Palette,
  Puzzle,
  Quote,
  Radio,
  ScrollText,
  Send,
  Settings,
  ShieldAlert,
  ShieldCheck,
  SlidersHorizontal,
  Smile,
  Sparkles,
  Swords,
  TicketCheck,
  Trophy,
  UserCog,
  UserMinus,
  Users,
  Volume2,
  Vote,
  Wand2,
};

export function resolveIcon(name: string): LucideIcon {
  return REGISTRY[name] ?? Puzzle;
}

export function Icon({
  name,
  className,
  strokeWidth = 1.75,
}: {
  name: string;
  className?: string;
  strokeWidth?: number;
}) {
  const Resolved = resolveIcon(name);
  return <Resolved className={className} strokeWidth={strokeWidth} aria-hidden />;
}
