/**
 * Turns a dotted audit action code into a line a person can read.
 *
 * The audit page exists to be read after the fact, so "ticketing.panel.create"
 * is a failure of that page even though it is technically accurate. This maps
 * the codes the app actually emits to plain phrases — and, crucially, falls
 * back to a generic humaniser so an action added later still reads as English
 * instead of leaking its raw code. That fallback is what stops this list from
 * silently going stale the way the old two-entry map did.
 */

/** Readable names for the action domains (the part before the first dot). */
const CATEGORY_LABELS: Record<string, string> = {
  settings: "Settings",
  module: "Modules",
  automation: "Automations",
  automations: "Automations",
  bot: "Bot control",
  "game-poll": "Game poll",
  nhie: "Never Have I Ever",
  qotd: "Question of the day",
  ticketing: "Ticketing",
  custommatch: "Custom matches",
  onboarding: "Welcome & onboarding",
  utility: "Utility",
  "roles-and-alerts": "Roles & alerts",
};

/** A readable name for an action domain, for the audit filter menu. */
export function describeCategory(domain: string): string {
  return (
    CATEGORY_LABELS[domain] ??
    domain.replace(/[-_]/g, " ").replace(/^\w/, (c) => c.toUpperCase())
  );
}

/** Curated phrasing for the codes the action files emit today. */
const LABELS: Record<string, string> = {
  "settings.update": "Updated settings",
  "module.toggle": "Toggled a module",

  "automation.create": "Created an automation",
  "automation.edit": "Edited an automation",
  "automation.delete": "Deleted an automation",
  "automation.duplicate": "Duplicated an automation",
  "automation.state": "Changed an automation's state",
  "automations.pause": "Paused all automations",
  "automations.resume": "Resumed all automations",

  "bot.restart": "Restarted the bot",
  "bot.reload_all": "Reloaded every cog",

  "game-poll.game.add": "Added a game",
  "game-poll.game.update": "Edited a game",
  "game-poll.game.remove": "Removed a game",
  "game-poll.poll.post": "Posted a poll",
  "game-poll.poll.draft": "Saved a poll draft",

  "nhie.questions.add": "Added Never-Have-I-Ever questions",
  "nhie.question.delete": "Deleted a Never-Have-I-Ever question",
  "nhie.pool.reset": "Reset the Never-Have-I-Ever pool",

  "qotd.questions.add": "Added questions of the day",
  "qotd.question.edit": "Edited a question of the day",
  "qotd.question.delete": "Deleted a question of the day",
  "qotd.tomorrow.reroll": "Re-rolled tomorrow's question",
  "qotd.pool.reset": "Reset the question pool",
  "qotd.pool.clear_seen": "Cleared the asked-already history",

  "ticketing.topic.create": "Created a ticket topic",
  "ticketing.topic.update": "Updated a ticket topic",
  "ticketing.topic.delete": "Deleted a ticket topic",
  "ticketing.panel.create": "Created a ticket panel",
  "ticketing.panel.update": "Updated a ticket panel",
  "ticketing.panel.delete": "Deleted a ticket panel",
  "ticketing.panel.publish": "Published a ticket panel",
  "ticketing.panel.unpublish": "Unpublished a ticket panel",
  "ticketing.survey.send": "Sent a ticket survey",
  "ticketing.response.delete": "Deleted a saved response",
  "ticketing.responses.delete": "Deleted saved responses",

  "custommatch.game.add": "Added a match game",
  "custommatch.game.update": "Edited a match game",
  "custommatch.game.clone": "Cloned a match game",
  "custommatch.game.delete": "Deleted a match game",
  "custommatch.global.set": "Changed global match settings",
  "custommatch.player.mmr": "Adjusted a player's MMR",
  "custommatch.player.offset": "Adjusted a player's MMR offset",
  "custommatch.player.ign": "Changed a player's in-game name",
  "custommatch.rank.set": "Set a rank",
  "custommatch.rank.remove": "Removed a rank",
  "custommatch.penalty.clear": "Cleared a penalty",
  "custommatch.suspension.remove": "Lifted a suspension",
  "custommatch.blacklist.add": "Blacklisted a player",
  "custommatch.blacklist.remove": "Un-blacklisted a player",
  "custommatch.modrole.add": "Added a match mod role",
  "custommatch.modrole.remove": "Removed a match mod role",

  "onboarding.question.add": "Added an intro question",
  "onboarding.question.edit": "Edited an intro question",
  "onboarding.question.delete": "Deleted an intro question",
  "onboarding.questions.reorder": "Reordered the intro questions",
  "onboarding.mapping.add": "Added a game→LFG mapping",
  "onboarding.mapping.remove": "Removed a game→LFG mapping",
  "onboarding.blacklist.add": "Blacklisted a member from onboarding",
  "onboarding.blacklist.remove": "Un-blacklisted a member",
  "onboarding.points.wipe_member": "Wiped a member's points",
  "onboarding.points.wipe_all": "Wiped everyone's points",

  "roles-and-alerts.settings.update": "Updated roles & alerts settings",
  "roles-and-alerts.shuffle.create": "Added a role shuffle",
  "roles-and-alerts.shuffle.delete": "Removed a role shuffle",
};

/** Last-segment verb → past tense, for the generic fallback. */
const VERBS: Record<string, string> = {
  add: "Added",
  create: "Created",
  update: "Updated",
  edit: "Edited",
  delete: "Deleted",
  remove: "Removed",
  set: "Set",
  clear: "Cleared",
  reset: "Reset",
  toggle: "Toggled",
  state: "Toggled",
  send: "Sent",
  post: "Posted",
  publish: "Published",
  unpublish: "Unpublished",
  reorder: "Reordered",
  clone: "Cloned",
  duplicate: "Duplicated",
  pause: "Paused",
  resume: "Resumed",
};

/**
 * A readable description of an audit action code — the curated phrase when we
 * have one, otherwise a best-effort "Verb — the rest of the code" so a code we
 * forgot to map still reads as a sentence rather than as `foo.bar.baz`.
 */
export function describeAction(action: string): string {
  const known = LABELS[action];
  if (known) return known;

  const parts = action.split(".");
  const last = parts[parts.length - 1] ?? "";
  const verb = VERBS[last];
  const rest = (verb ? parts.slice(0, -1) : parts).join(" ").replace(/[-_]/g, " ").trim();
  if (verb && rest) return `${verb} — ${rest}`;
  if (verb) return verb;
  // Nothing recognisable: at least strip the dots so it reads as words.
  return action.replace(/[.\-_]/g, " ");
}
