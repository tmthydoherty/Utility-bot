import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { isAvailable } from "@/lib/onboarding/store";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";

/**
 * The frame shared by the four Welcome & Onboarding rich-state sub-pages
 * (Questions, Mappings, Points, Blacklist).
 *
 * Each is a slice of the bot's published snapshot, so the back link, the header
 * and the "Vibey isn't running" notice live here rather than being copied into
 * four near-identical pages. Every edit inside still queues a command the cog
 * drains within about ten seconds.
 */
export function OnboardingSectionFrame({
  guildId,
  title,
  description,
  children,
}: {
  guildId: string;
  title: string;
  description: string;
  children: React.ReactNode;
}) {
  const botOnline = isAvailable();

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules/welcome`}>
          <ArrowLeft aria-hidden />
          Welcome &amp; Onboarding
        </Link>
      </Button>

      <PageHeader title={title} description={description} />

      {!botOnline && (
        <Badge variant="warning">
          Vibey isn&apos;t running — the lists below are the last snapshot, and changes apply when it
          starts.
        </Badge>
      )}

      {children}
    </div>
  );
}
