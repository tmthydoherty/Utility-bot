"""Run every utility and automations check. Exits non-zero if any of them fail.

    .venv/bin/python tests/run_all.py

Each check is a standalone script, run in its own process. That is deliberate:
several of them repoint a module-level DB_PATH or hand a fake Discord to code
that expects a real one, and a shared interpreter would let one suite's
monkey-patching leak into the next.
"""
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

CHECKS = [
    ("utility", "check_migration"),
    ("utility", "check_features"),
    ("utility", "check_panel"),
    ("automations", "check_migration"),
    ("automations", "check_engine"),
    ("automations", "check_panel"),
    ("automations", "check_readiness"),
    ("automations", "check_dispatch"),
    ("module_config", "check_sync"),
    ("custom_commands", "check_commands"),
]

results = []
for suite, name in CHECKS:
    label = f"{suite}/{name}"
    print(f"\n{'=' * 60}\n{label}\n{'=' * 60}")
    proc = subprocess.run([sys.executable, str(HERE / suite / f"{name}.py")],
                          capture_output=True, text=True, timeout=300)
    passed = proc.stdout.count("\n  ok   ")
    failed = proc.stdout.count("\n  FAIL ")
    results.append((label, proc.returncode, passed, failed))
    if proc.returncode != 0:
        print(proc.stdout[-4000:])
        print(proc.stderr[-2000:])
    else:
        print(f"{passed} checks passed")

print(f"\n{'=' * 60}\nSummary\n{'=' * 60}")
total_passed = sum(p for _, _, p, _ in results)
total_failed = sum(f for _, _, _, f in results)
for label, code, passed, failed in results:
    status = "PASS" if code == 0 else "FAIL"
    print(f"  {status}  {label}: {passed} passed, {failed} failed")
print(f"\n{total_passed} checks passed, {total_failed} failed")

sys.exit(1 if any(code != 0 for _, code, _, _ in results) else 0)
