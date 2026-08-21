"use client";

import * as React from "react";
import { Braces } from "lucide-react";

import { cn } from "@/lib/utils";
import { Sheet } from "@/components/ui/sheet";

/**
 * The cheat sheet for what you can write inside a message.
 *
 * Mirrors `PLACEHOLDER_HELP` in utils/placeholders.py. Nobody discovers
 * `{user.mention}` by guessing, and the Discord panel had exactly this problem
 * until a button was added: the shortcuts existed but nothing on screen ever
 * mentioned them, so the only way to find one was to be told.
 *
 * It sits directly under the field it applies to, not in a docs page, because
 * the moment you need it is the moment you are typing the message.
 */

const GROUPS: [string, [string, string][]][] = [
  [
    "Links and pings",
    [
      ["{#channel-name}", "A clickable link to that channel, like {#rules}"],
      ["{@role-name}", "Pings that role, like {@Moderators}"],
      ["{user.mention}", "Pings the person who set this off"],
      ["{channel.mention}", "The channel it happened in"],
      ["{message.link}", "A jump link straight to their message"],
    ],
  ],
  [
    "About them",
    [
      ["{user.name}", "Their username"],
      ["{user.display}", "Their nickname here"],
      ["{user.avatar}", "Their profile picture — for the picture fields"],
      ["{user.joined_days}", "Days since they joined the server"],
      ["{user.account_days}", "How old their Discord account is, in days"],
      ["{user.id}", "Their user ID"],
    ],
  ],
  [
    "About the server",
    [
      ["{guild.name}", "The server's name"],
      ["{guild.members}", "How many members it has"],
      ["{channel.name}", "The channel name, without a link"],
    ],
  ],
  [
    "About the message",
    [
      ["{message.content}", "What they actually wrote"],
      ["{match.1}", "Text captured by a search-pattern requirement"],
    ],
  ],
  [
    "Anything else",
    [
      ["{counter.name}", "The current value of a tally"],
      ["{var.name}", "Something jotted down earlier in this run"],
      ["{random.1-100}", "A random number in that range"],
    ],
  ],
];

export function PlaceholderHelp({ className }: { className?: string }) {
  const [open, setOpen] = React.useState(false);

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className={cn(
          "mt-1.5 inline-flex items-center gap-1.5 text-xs font-medium",
          "text-[var(--accent)] transition-opacity hover:opacity-80",
          className,
        )}
      >
        <Braces className="size-3.5" aria-hidden />
        What can I put in here?
      </button>

      <Sheet
        open={open}
        onOpenChange={setOpen}
        title="What can I put in here?"
        description="Type any of these and Vibey fills them in when it sends the message."
        size="lg"
      >
        <div className="space-y-5">
          <div className="rounded-lg border border-[var(--border)] bg-[var(--bg-inset)] p-3 text-sm">
            <p className="font-medium">For example</p>
            <p className="mt-1.5 font-mono text-xs leading-relaxed text-fg-muted">
              Welcome {"{user.mention}"}! Please read {"{#rules}"} and say hi in {"{#general}"}.
            </p>
            <p className="mt-2 text-xs text-fg-subtle">
              …sends as <em>Welcome @Ash! Please read #rules and say hi in #general.</em>
            </p>
          </div>

          {GROUPS.map(([heading, entries]) => (
            <section key={heading} className="space-y-1.5">
              <h3 className="text-xs font-semibold uppercase tracking-wide text-fg-subtle">
                {heading}
              </h3>
              <dl className="space-y-1.5">
                {entries.map(([token, what]) => (
                  <div key={token} className="flex flex-col gap-0.5 sm:flex-row sm:gap-3">
                    <dt className="shrink-0 font-mono text-xs text-[var(--accent)] sm:w-44">
                      {token}
                    </dt>
                    <dd className="text-xs text-fg-muted">{what}</dd>
                  </div>
                ))}
              </dl>
            </section>
          ))}

          <p className="text-xs text-fg-subtle">
            Channel and role names don&apos;t have to be exact — {"{#rules}"} finds
            📜-server-rules.
          </p>
        </div>
      </Sheet>
    </>
  );
}
