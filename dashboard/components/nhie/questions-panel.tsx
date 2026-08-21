import { readPool, isAvailable } from "@/lib/nhie/store";
import { NhieQuestions } from "./questions";

export function NhieQuestionsPanel({ guildId }: { guildId: string }) {
  const pool = readPool(guildId);
  const botOnline = isAvailable();

  return (
    <section className="space-y-4">
      <div>
        <h2 className="text-lg font-semibold">Question pool</h2>
        <p className="text-sm text-fg-muted">
          The questions Vibey draws from for Never Have I Ever rounds. Add to them, or edit the list.
        </p>
      </div>
      <NhieQuestions
        guildId={guildId}
        pool={pool}
        botOnline={botOnline}
      />
    </section>
  );
}
