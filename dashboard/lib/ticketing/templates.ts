import { topicDefaults, type Topic } from "./types";

/**
 * Ready-made topics for the setup flow.
 *
 * A first-time admin should get a working topic without reading forty fields, so
 * each preset fills in a sensible starting point they can then adjust. The
 * consequential fields — where tickets land, who staffs them — are deliberately
 * left blank so the setup checklist points at them rather than the bot silently
 * using the wrong channel.
 */

export interface TopicTemplate {
  key: string;
  name: string;
  label: string;
  /** One line, plain language — what this topic is for. */
  blurb: string;
  /** lucide-react icon name. */
  icon: string;
  build: () => Topic;
}

export const TOPIC_TEMPLATES: TopicTemplate[] = [
  {
    key: "support",
    name: "support",
    label: "Support",
    blurb: "A general help ticket members open when they need a hand.",
    icon: "LifeBuoy",
    build: () => ({
      ...topicDefaults("support"),
      label: "Support",
      emoji: "🎫",
      type: "ticket",
      mode: "thread",
      buttonColor: "primary",
      welcomeMessage:
        "Thanks for reaching out, {user}! Tell us what you need and a staff member will help.\nTopic: **{topic}**",
    }),
  },
  {
    key: "application",
    name: "staff-application",
    label: "Staff Application",
    blurb: "A set of questions applicants answer, sent to staff to review.",
    icon: "ClipboardList",
    build: () => ({
      ...topicDefaults("staff-application"),
      label: "Staff Application",
      emoji: "📋",
      type: "application",
      mode: "channel",
      buttonColor: "success",
      approvalMode: true,
      applicationChannelMode: "dm",
      questions: [
        "How old are you?",
        "What timezone are you in?",
        "Why do you want to join the staff team?",
        "How much time can you give each week?",
      ],
    }),
  },
  {
    key: "feedback",
    name: "feedback",
    label: "Feedback",
    blurb: "A short survey members fill in — answers are stored, no ticket opens.",
    icon: "MessageSquareHeart",
    build: () => ({
      ...topicDefaults("feedback"),
      label: "Feedback",
      emoji: "💬",
      type: "survey",
      mode: "thread",
      buttonColor: "secondary",
      applicationChannelMode: "dm",
      questions: [
        "What do you enjoy most about the server?",
        "What is one thing we could do better?",
      ],
    }),
  },
];

export function getTopicTemplate(key: string): TopicTemplate | undefined {
  return TOPIC_TEMPLATES.find((t) => t.key === key);
}
