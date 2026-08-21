import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { listShuffleTasks } from "@/lib/roles-and-alerts/store";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/ui/page-header";
import { ShuffleForm } from "./form";
import { ShuffleList } from "./list";

export const metadata: Metadata = { title: "Role Shuffle" };

export default async function ShufflePage({
  params,
}: {
  params: Promise<{ guildId: string }>;
}) {
  const { guildId } = await params;
  const tasks = listShuffleTasks(guildId);

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-3">
        <Link href={`/dashboard/${guildId}/modules/roles-and-alerts`}>
          <ArrowLeft aria-hidden />
          Roles &amp; Alerts
        </Link>
      </Button>

      <PageHeader
        title="Role Shuffle"
        description="Bulk assign or remove a role from members matching specific trigger roles."
      />

      <div className="grid gap-6 md:grid-cols-[400px_1fr]">
        <ShuffleForm guildId={guildId} />
        <ShuffleList guildId={guildId} tasks={tasks} />
      </div>
    </div>
  );
}
