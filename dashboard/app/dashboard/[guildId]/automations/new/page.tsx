import type { Metadata } from "next";

import { TemplateGallery } from "@/components/automations/template-gallery";
import { PageHeader } from "@/components/ui/page-header";

export const metadata: Metadata = { title: "New automation" };

export default async function NewAutomationPage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;

  return (
    <div className="space-y-6">
      <PageHeader
        title="What should Vibey do?"
        description="Pick something close to what you want. It arrives already set up and switched off, and you change the bits you disagree with."
      />
      <TemplateGallery guildId={guildId} />
    </div>
  );
}
