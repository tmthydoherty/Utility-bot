/**
 * Starting points for a new game, so spinning one up isn't forty blank fields.
 *
 * Each preset is just the four `add_game` arguments plus any extra columns worth
 * defaulting (Overwatch wants role-queue on). The bot's `add_game` already turns
 * an "Overwatch"-named game into a role-queue with seeded weights, so the OW
 * preset's `role_required` is belt-and-braces rather than the only thing doing it.
 *
 * "Blank" is a real preset — an empty name the operator fills in — so the dialog
 * has a single, uniform path whether they start from a template or from scratch.
 */
export interface GamePreset {
  key: string;
  label: string;
  description: string;
  name: string;
  playerCount: number;
  queueType: string;
  captainSelection: string;
  fields?: Record<string, unknown>;
}

export const GAME_PRESETS: GamePreset[] = [
  {
    key: "valorant",
    label: "Valorant 5v5",
    description: "Ten players, MMR-balanced teams.",
    name: "Valorant",
    playerCount: 10,
    queueType: "mmr",
    captainSelection: "random",
  },
  {
    key: "cs",
    label: "CS2 5v5",
    description: "Ten players, MMR-balanced teams.",
    name: "CS2",
    playerCount: 10,
    queueType: "mmr",
    captainSelection: "random",
  },
  {
    key: "overwatch",
    label: "Overwatch 6v6",
    description: "Twelve players, role queue, strict 2-2-2.",
    name: "Overwatch",
    playerCount: 12,
    queueType: "mmr",
    captainSelection: "random",
    fields: { role_required: true },
  },
  {
    key: "rivals",
    label: "Marvel Rivals 6v6",
    description: "Twelve players, MMR-balanced teams.",
    name: "Marvel Rivals",
    playerCount: 12,
    queueType: "mmr",
    captainSelection: "random",
  },
  {
    key: "lol",
    label: "League 5v5",
    description: "Ten players, MMR-balanced teams.",
    name: "League of Legends",
    playerCount: 10,
    queueType: "mmr",
    captainSelection: "random",
  },
  {
    key: "captains",
    label: "Captain draft",
    description: "Ten players, two captains draft the teams.",
    name: "Customs",
    playerCount: 10,
    queueType: "captains",
    captainSelection: "highest_mmr",
  },
  {
    key: "blank",
    label: "Blank",
    description: "Start from scratch and set everything yourself.",
    name: "",
    playerCount: 10,
    queueType: "mmr",
    captainSelection: "random",
  },
];
