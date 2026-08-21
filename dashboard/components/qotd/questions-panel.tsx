import { readPool, isAvailable } from "@/lib/qotd/store";
import { readSettings } from "@/lib/bot/adapter";
import { QotdQuestions } from "./questions";

/**
 * Server half of the QOTD pool manager: reads the bot's question snapshot and
 * whether automatic posting is on, then hands both to the client component.
 * Rendered below the settings form on the QOTD module page — the pool is part
 * of the module, not a separate builder, so it lives on the same page.
 */
export function QotdQuestionsPanel({ guildId }: { guildId: string }) {
  const pool = readPool(guildId);
  const botOnline = isAvailable();
  const autoPosting = readSettings(guildId, "qotd").enabled === true;

  return (
    <section className="space-y-4">
      <div>
        <h2 className="text-lg font-semibold">Question pool</h2>
        <p className="text-sm text-fg-muted">
          The questions Vibey draws from, shared across the server. Add to them, edit the list, and
          see what&apos;s queued up next.
        </p>
      </div>
      <QotdQuestions
        guildId={guildId}
        pool={pool}
        botOnline={botOnline}
        autoPosting={autoPosting}
      />
    </section>
  );
}
