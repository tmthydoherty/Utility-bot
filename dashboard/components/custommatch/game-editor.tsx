"use client";

import * as React from "react";

import { cn } from "@/lib/utils";
import {
  isVisible,
  missingRequired,
  type FieldValue,
  type ModuleSchema,
  type Section,
  type SettingsValues,
} from "@/lib/schema/types";
import { validateModule } from "@/lib/schema/validate";
import { GAME_SECTIONS } from "@/lib/custommatch/game-schema";
import type { CmGame, CmRank } from "@/lib/custommatch/read";
import { updateGame } from "@/app/actions/custommatch";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { SaveBar } from "@/components/ui/save-bar";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useToast } from "@/components/ui/toast";
import { FieldRow } from "@/components/settings/field-renderer";
import { RankLadderPanel } from "./rank-ladder-panel";
import { ScheduleEditor } from "./schedule-editor";
import { DangerZone } from "./danger-zone";

/**
 * The whole editor for one game.
 *
 * The flat settings (rules, queue, channels, penalties, fun modes, appearance,
 * and the schedule's on/off + hours) are one buffered working copy against a
 * saved baseline — exactly the model `ModuleSettingsForm` uses — so the SaveBar,
 * the change count and the discard all derive from comparing the two and can't
 * fall out of sync. A save sends only the changed columns through `updateGame`.
 *
 * The rank ladder and the danger zone aren't buffered: they apply immediately
 * through their own bridge commands, so they live in their own tabs with their
 * own controls and sit outside the SaveBar.
 */

// The buffered keys: every flat field, plus the schedule pair the Schedule tab
// edits with bespoke controls rather than a FieldRow.
const FLAT_KEYS = GAME_SECTIONS.flatMap((s) => s.fields.map((f) => f.key));
const BUFFERED_KEYS = [...FLAT_KEYS, "schedule_enabled", "schedule_times"];

const SECTION_BY_ID = new Map(GAME_SECTIONS.map((s) => [s.id, s]));

// A synthetic module so the shared validator and required-field logic apply
// unchanged to the flat sections.
const GAME_MODULE: ModuleSchema = {
  id: "custommatch-game",
  name: "Game",
  description: "",
  icon: "Swords",
  category: "esports",
  cog: "cogs/custommatch/",
  configurable: true,
  sections: GAME_SECTIONS,
};

const TAB_ORDER: { id: string; label: string }[] = [
  { id: "rules", label: "Rules" },
  { id: "queue", label: "Queue" },
  { id: "channels", label: "Channels" },
  { id: "ranks", label: "Rank ladder" },
  { id: "schedule", label: "Schedule" },
  { id: "penalties", label: "Penalties" },
  { id: "funmodes", label: "Fun modes" },
  { id: "appearance", label: "Appearance" },
  { id: "danger", label: "Danger" },
];

function initialValues(game: CmGame): SettingsValues {
  const values: SettingsValues = {};
  const record = game as unknown as Record<string, FieldValue>;
  for (const key of BUFFERED_KEYS) {
    values[key] = (record[key] ?? null) as FieldValue;
  }
  return values;
}

export function GameEditor({
  guildId,
  game,
  ranks,
  botOnline,
}: {
  guildId: string;
  game: CmGame;
  ranks: CmRank[];
  botOnline: boolean;
}) {
  const toast = useToast();
  const gameId = game.game_id;

  const [saved, setSaved] = React.useState<SettingsValues>(() => initialValues(game));
  const [values, setValues] = React.useState<SettingsValues>(() => initialValues(game));
  const [errors, setErrors] = React.useState<Record<string, string>>({});
  const [saving, setSaving] = React.useState(false);
  const [saveError, setSaveError] = React.useState<string | null>(null);

  const changedKeys = React.useMemo(
    () => BUFFERED_KEYS.filter((key) => JSON.stringify(values[key]) !== JSON.stringify(saved[key])),
    [values, saved],
  );
  const dirty = changedKeys.length > 0;

  React.useEffect(() => {
    if (!dirty) return;
    const warn = (event: BeforeUnloadEvent) => event.preventDefault();
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [dirty]);

  const setValue = (key: string, value: FieldValue) => {
    setValues((current) => ({ ...current, [key]: value }));
    setErrors((current) => {
      if (!current[key]) return current;
      const next = { ...current };
      delete next[key];
      return next;
    });
  };

  const reset = () => {
    setValues(saved);
    setErrors({});
    setSaveError(null);
  };

  const save = async () => {
    const local = validateModule(GAME_MODULE, values);
    if (!local.ok) {
      setErrors(local.errors);
      setSaveError("Some fields need attention.");
      const firstKey = Object.keys(local.errors)[0];
      if (firstKey) {
        document.getElementById(`field-${firstKey}`)?.scrollIntoView({ behavior: "smooth", block: "center" });
      }
      return;
    }

    const fields: Record<string, unknown> = {};
    for (const key of changedKeys) fields[key] = values[key];

    setSaving(true);
    setSaveError(null);
    try {
      const result = await updateGame(guildId, gameId, fields);
      if (result.ok) {
        setSaved(values);
        setErrors({});
        toast.success("Saved", "Vibey applies this within about 10 seconds.");
      } else {
        setSaveError(result.error ?? "Something went wrong.");
      }
    } catch {
      setSaveError("Couldn't reach the server. Your changes are still here.");
    } finally {
      setSaving(false);
    }
  };

  const incomplete = missingRequired(GAME_MODULE, values);

  return (
    <div className="space-y-5 pb-28 lg:pb-24">
      {!botOnline && (
        <Badge variant="warning">Vibey isn&apos;t running — changes apply when it starts</Badge>
      )}

      {incomplete.length > 0 && (
        <Card className="border-[var(--warning)]/30 bg-[var(--warning-soft)] p-4">
          <p className="text-sm font-medium text-[var(--warning)]">
            {incomplete.length} required {incomplete.length === 1 ? "setting is" : "settings are"} still empty
          </p>
          <p className="mt-1 text-sm text-fg-muted">
            {incomplete.map((field) => field.label).join(", ")}
          </p>
        </Card>
      )}

      <Tabs defaultValue="rules">
        <TabsList className="flex-wrap">
          {TAB_ORDER.map((tab) => (
            <TabsTrigger key={tab.id} value={tab.id}>
              {tab.label}
            </TabsTrigger>
          ))}
        </TabsList>

        {/* Flat, buffered sections */}
        {(["rules", "queue", "channels", "penalties", "funmodes", "appearance"] as const).map((id) => {
          const section = SECTION_BY_ID.get(id) as Section;
          return (
            <TabsContent key={id} value={id}>
              <FlatSection section={section} values={values} errors={errors} onChange={setValue} disabled={saving} />
            </TabsContent>
          );
        })}

        {/* Rank ladder — applies immediately, outside the SaveBar */}
        <TabsContent value="ranks">
          <RankLadderPanel guildId={guildId} gameId={gameId} ranks={ranks} />
        </TabsContent>

        {/* Schedule — the on/off and hours are buffered with the rest */}
        <TabsContent value="schedule">
          <Card className="space-y-4 p-5 sm:p-6">
            <div className="flex items-center justify-between gap-4">
              <div className="min-w-0">
                <p className="text-sm font-medium">Automatic schedule</p>
                <p className="text-sm text-fg-muted">
                  Open and close the queue on a weekly timetable instead of by hand.
                </p>
              </div>
              <Switch
                checked={Boolean(values.schedule_enabled)}
                disabled={saving}
                onCheckedChange={(checked) => setValue("schedule_enabled", checked)}
                aria-label="Automatic schedule"
              />
            </div>
            {Boolean(values.schedule_enabled) && (
              <ScheduleEditor
                value={(values.schedule_times as Record<string, { open: string; close: string }> | null) ?? null}
                onChange={(next) => setValue("schedule_times", next as unknown as FieldValue)}
                disabled={saving}
              />
            )}
          </Card>
        </TabsContent>

        {/* Danger zone — clone and delete, immediate */}
        <TabsContent value="danger">
          <DangerZone guildId={guildId} gameId={gameId} gameName={game.name} />
        </TabsContent>
      </Tabs>

      <SaveBar
        visible={dirty}
        saving={saving}
        error={saveError}
        changeCount={changedKeys.length}
        onSave={save}
        onReset={reset}
      />
    </div>
  );
}

function FlatSection({
  section,
  values,
  errors,
  onChange,
  disabled,
}: {
  section: Section;
  values: SettingsValues;
  errors: Record<string, string>;
  onChange: (key: string, value: FieldValue) => void;
  disabled: boolean;
}) {
  return (
    <Card>
      <div className="space-y-1 p-5 pb-0 sm:p-6 sm:pb-0">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-fg-muted">
          {section.title}
        </h2>
        {section.description && <p className="text-sm text-fg-muted">{section.description}</p>}
      </div>
      <div className={cn("grid gap-5 p-5 sm:grid-cols-2 sm:p-6")}>
        {section.fields.map((field) =>
          isVisible(field, values) ? (
            <FieldRow
              key={field.key}
              field={field}
              value={values[field.key] ?? null}
              onChange={(value) => onChange(field.key, value)}
              error={errors[field.key]}
              disabled={disabled}
            />
          ) : null,
        )}
      </div>
    </Card>
  );
}
