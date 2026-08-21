"""Verify cross-cog events match their listeners.

`bot.dispatch("x", a, b)` and `async def on_x(self, a, b, c)` is a mismatch
discord.py reports by logging an exception inside the event loop and carrying
on — the automation simply never fires, with nothing in the panel to explain
why. Nothing at import time catches it, so it is checked here instead.

This reads the source rather than running the bot, so it works without a
token and covers every dispatch site including ones on error paths.

Run: .venv/bin/python tests/automations/check_dispatch.py
"""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

FAILURES = []


def check(condition, message):
    if condition:
        print(f"  ok   {message}")
    else:
        print(f"  FAIL {message}")
        FAILURES.append(message)


def find_dispatches(path: Path) -> list:
    """Every `<something>.dispatch("name", ...)` with its positional arity."""
    found = []
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "dispatch"):
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        name = node.args[0].value
        if not isinstance(name, str):
            continue
        found.append((name, len(node.args) - 1, f"{path.name}:{node.lineno}"))
    return found


def find_listeners(path: Path) -> dict:
    """Every `async def on_x` decorated as a Cog listener, with its arity."""
    listeners = {}
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        if not node.name.startswith("on_"):
            continue
        decorated = any(
            isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and d.func.attr == "listener"
            for d in node.decorator_list
        )
        if not decorated:
            continue
        args = node.args
        required = len(args.args) - 1 - len(args.defaults)   # minus `self`
        optional = len(args.defaults)
        listeners[node.name[3:]] = (required, optional, f"{path.name}:{node.lineno}")
    return listeners


print("\nScanning")
cogs_dir = ROOT / "cogs"
sources = sorted(p for p in cogs_dir.rglob("*.py") if "__pycache__" not in str(p))

all_dispatches = {}
for source in sources:
    for name, arity, where in find_dispatches(source):
        all_dispatches.setdefault(name, []).append((arity, where))

automation_listeners = find_listeners(cogs_dir / "automations" / "cog.py")
print(f"  found {len(all_dispatches)} custom event(s), "
      f"{len(automation_listeners)} listener(s) in the automations cog")

# The events the automations engine depends on. `member_level_up` predates
# this work and is dispatched by the economy cog.
CROSS_COG_EVENTS = [
    "member_level_up", "ticket_opened", "ticket_closed",
    "newcomer_intro_posted", "newcomer_graduated",
    "vc_lobby_created", "vc_lobby_closed",
]

print("\nEvery cross-cog trigger is actually dispatched somewhere")
for event in CROSS_COG_EVENTS:
    sites = all_dispatches.get(event, [])
    check(bool(sites), f"{event} is dispatched ({[w for _, w in sites] or 'NOWHERE'})")

print("\nEvery cross-cog trigger has a listener")
for event in CROSS_COG_EVENTS:
    check(event in automation_listeners, f"the automations cog listens for {event}")

print("\nArity matches between dispatch and listener")
for event in CROSS_COG_EVENTS:
    sites = all_dispatches.get(event, [])
    listener = automation_listeners.get(event)
    if not sites or listener is None:
        continue
    required, optional, where = listener
    for arity, site in sites:
        ok = required <= arity <= required + optional
        check(ok, f"{event}: dispatch at {site} passes {arity} arg(s), "
                  f"listener at {where} takes {required}"
                  f"{f'-{required + optional}' if optional else ''}")

print("\nAll dispatch sites of one event agree with each other")
for event, sites in sorted(all_dispatches.items()):
    arities = {arity for arity, _ in sites}
    check(len(arities) == 1,
          f"{event} dispatched with a consistent arity "
          f"({sorted(arities)} at {[w for _, w in sites]})")

print("\nRegistry triggers line up with listeners")
from cogs.automations.registry import TRIGGERS

# Driven by the explicit `dispatched` flag rather than the display category:
# categories get regrouped for readability, and a test that keys off them
# starts failing for cosmetic reasons while missing real breakage.
dispatched = {k for k, s in TRIGGERS.items() if s.dispatched}
check(dispatched == set(CROSS_COG_EVENTS),
      f"the registry's dispatched triggers match this test's list "
      f"(registry: {sorted(dispatched)})")
for key in sorted(dispatched):
    check(key in automation_listeners,
          f"trigger '{key}' has a listener to feed it")
    check(key in all_dispatches,
          f"trigger '{key}' is dispatched by some cog")

print("\nNon-dispatched triggers are native Discord events")
for key, spec in TRIGGERS.items():
    if spec.dispatched or key in ("schedule", "manual"):
        continue
    check(key not in all_dispatches,
          f"'{key}' comes from Discord directly, so nothing should dispatch it")

print("\n" + "=" * 60)
if FAILURES:
    print(f"{len(FAILURES)} FAILURE(S):")
    for failure in FAILURES:
        print(f"  - {failure}")
    sys.exit(1)
print("All dispatch checks passed.")
