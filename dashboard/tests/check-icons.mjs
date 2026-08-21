/**
 * Every icon a schema names must be in the registry.
 *
 * `resolveIcon` falls back to a neutral puzzle piece for an unknown name, which
 * is the right behaviour at runtime — a missing icon must not crash a page —
 * and it means a typo produces no error of any kind. The automations section
 * shipped its first build with sixty-one unregistered names, so every trigger,
 * check, step and ready-made automation rendered the same grey puzzle piece.
 * It looked deliberate. Nothing in TypeScript, ESLint or the build said a word.
 *
 * So the check is here instead. It reads the source rather than importing it,
 * because the registry is a .tsx module and this needs to run under plain node.
 *
 * Run: node tests/check-icons.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

// Every file that names an icon as a string for `<Icon name=…>` to resolve.
const SOURCES = [
  "lib/automations/registry.ts",
  "lib/automations/templates.ts",
  "lib/schema/modules.ts",
  "lib/nav.ts",
];

const failures = [];
const check = (condition, message) => {
  console.log(`  ${condition ? "ok  " : "FAIL"} ${message}`);
  if (!condition) failures.push(message);
};

const registry = fs.readFileSync(path.join(ROOT, "components/ui/icon.tsx"), "utf8");
const registered = (name) => new RegExp(`\\n  ${name}[,:]`).test(registry);

console.log("\nEvery icon named in a schema is in the registry");
let total = 0;
for (const source of SOURCES) {
  const text = fs.readFileSync(path.join(ROOT, source), "utf8");
  const names = [...new Set([...text.matchAll(/icon:\s*"([A-Za-z0-9]+)"/g)].map((m) => m[1]))];
  total += names.length;
  const missing = names.filter((name) => !registered(name));
  check(missing.length === 0, `${source}: ${names.length} icon(s) — ${missing.join(", ") || "all registered"}`);
}
check(total > 0, `found ${total} icon names to check (a zero here means the pattern stopped matching)`);

console.log("\n" + "=".repeat(60));
if (failures.length) {
  console.log(`${failures.length} FAILURE(S):`);
  for (const failure of failures) console.log(`  - ${failure}`);
  console.log("\nAdd the missing names to the REGISTRY in components/ui/icon.tsx.");
  process.exit(1);
}
console.log("All icon checks passed.");
