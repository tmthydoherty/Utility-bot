import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { welcome } from "@/lib/schema/modules";
import { ONBOARDING_TILES } from "@/lib/onboarding/tabs";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { PageHeader } from "@/components/ui/page-header";

export const metadata: Metadata = { title: welcome.name };

export default async function WelcomeModulePage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules`}>
          <ArrowLeft aria-hidden />
          All modules
        </Link>
      </Button>

      <PageHeader title={welcome.name} description={welcome.description} />

      <div className="grid gap-4 sm:grid-cols-2">
        {ONBOARDING_TILES.map((tile) => (
          <Card key={tile.slug} interactive className="group relative p-5 sm:p-6">
            <div className="flex items-start gap-4">
              <div className="grid size-11 shrink-0 place-items-center rounded-lg bg-[var(--accent-soft)] text-[var(--accent)]">
                <Icon name={tile.icon} className="size-5" />
              </div>
              <div className="min-w-0 flex-1">
                <h2 className="font-semibold leading-tight">
                  <Link
                    href={`/dashboard/${guildId}/modules/welcome/${tile.slug}`}
                    className="after:absolute after:inset-0 after:content-['']"
                  >
                    {tile.title}
                  </Link>
                </h2>
                <p className="mt-1 text-sm leading-relaxed text-fg-muted">{tile.description}</p>
              </div>
              <Icon
                name="ArrowRight"
                className="size-4 shrink-0 text-fg-subtle transition-transform duration-200 group-hover:translate-x-0.5"
              />
            </div>
          </Card>
        ))}
      </div>

      {/* The source of truth in Discord, kept out of the way — provenance, not
          something to act on. */}
      <p className="pt-2 text-center font-mono text-xs text-fg-subtle/60">{welcome.cog}</p>
    </div>
  );
}
