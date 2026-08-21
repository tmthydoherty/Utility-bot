"""Evaluating and running automations.

The engine's job is to take an event, decide which automations care, and walk
their steps — recording as it goes *why* each decision went the way it did.
That trace is the whole reason the "why didn't it fire?" screen can exist: an
automation that silently does nothing is the single most common complaint
about MEE6's, and the answer is almost always one condition the admin has
misremembered.

Safety, in the order it is enforced:

1. the global kill switch, then the automation's own `enabled` flag;
2. bot-authored events, which are dropped unless explicitly opted in — this is
   what stops two automations answering each other forever;
3. the composition depth cap, so `run another automation` cannot recurse;
4. a per-automation rate budget, so a mistake costs a burst and not the API
   ratelimit for the whole bot;
5. the cooldown the admin configured.

`dry_run` is threaded through as `ctx.simulate`, and every action honours it.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import List, Optional, Tuple

from utils.events import EventContext
from .storage import now
from .models import Automation, ConditionGroup, ConditionLeaf, Step
from .registry import ACTIONS, CONDITIONS

logger = logging.getLogger('cogs.automations.engine')

MAX_DEPTH = 3               # how deep `run another automation` may chain
MAX_RUNS_PER_MINUTE = 30    # per automation
MAX_ACTIONS_PER_RUN = 25    # across branches, so a deep tree cannot run away

# automation_id -> timestamps of recent runs, for the rate budget.
_recent_runs: dict[str, deque] = {}


class Trace:
    """The record of one run, in the order things happened."""

    def __init__(self):
        self.entries: List[dict] = []

    def add(self, kind: str, label: str, result: Optional[bool], detail: str = ""):
        self.entries.append({
            "kind": kind, "label": label,
            "result": result, "detail": detail[:300],
        })

    def to_list(self) -> list:
        return self.entries[:200]

    def summary(self) -> str:
        for entry in reversed(self.entries):
            if entry["kind"] in ("action", "skip", "error"):
                return f"{entry['label']}: {entry['detail']}"
        return self.entries[-1]["detail"] if self.entries else ""


def _budget_ok(automation_id: str) -> bool:
    """A token bucket over the last minute."""
    bucket = _recent_runs.setdefault(automation_id, deque())
    cutoff = time.monotonic() - 60
    while bucket and bucket[0] < cutoff:
        bucket.popleft()
    if len(bucket) >= MAX_RUNS_PER_MINUTE:
        return False
    bucket.append(time.monotonic())
    return True


# --------------------------------------------------------------- conditions

async def evaluate(cog, ctx: EventContext, node, trace: Trace,
                   depth: int = 0) -> bool:
    """Walk a condition tree, recording each leaf's verdict and reason."""
    if node is None:
        return True

    if isinstance(node, ConditionGroup):
        if node.is_empty():
            # An empty group is "no conditions", which must mean "always" —
            # the alternative would make a brand-new automation never fire and
            # look broken.
            trace.add("condition", "no conditions", True, "matches everything")
            return True

        matched = node.op != "or"      # AND starts true, OR starts false
        for item in node.items:
            result = await evaluate(cog, ctx, item, trace, depth + 1)
            if node.op == "or":
                matched = matched or result
                # No short-circuit: the trace is more useful when it shows
                # every branch that was considered, and conditions are cheap.
            else:
                matched = matched and result
        if node.negate:
            trace.add("condition", f"NOT ({node.op})", not matched, "group negated")
            return not matched
        return matched

    spec = CONDITIONS.get(node.type)
    if spec is None or spec.run is None:
        trace.add("condition", node.type, False, "this condition no longer exists")
        return False

    try:
        matched, reason = await spec.run(cog, ctx, node.config or {})
    except Exception as e:
        logger.warning(f"Condition {node.type} raised: {e}", exc_info=True)
        trace.add("condition", spec.label, False, f"errored: {e}")
        return False

    if node.negate:
        matched = not matched
        reason = f"NOT — {reason}"
    trace.add("condition", spec.label, matched, reason)
    return matched


# -------------------------------------------------------------------- steps

class Stop(Exception):
    """Raised by the `stop` action to end a run cleanly."""


class Deferred(Exception):
    """Raised by `wait`: the rest of the run is persisted for the scheduler."""

    def __init__(self, seconds: int, remaining: List[Step]):
        super().__init__(f"waiting {seconds}s")
        self.seconds = seconds
        self.remaining = remaining


async def run_steps(cog, ctx: EventContext, steps: List[Step], trace: Trace,
                    budget: List[int]) -> None:
    for index, step in enumerate(steps):
        if budget[0] <= 0:
            trace.add("skip", "budget", False,
                      f"stopped after {MAX_ACTIONS_PER_RUN} actions")
            return
        budget[0] -= 1

        if step.is_branch:
            matched = await evaluate(cog, ctx, step.conditions, trace)
            branch = step.then if matched else step.otherwise
            trace.add("branch", "If", matched,
                      f"took the {'then' if matched else 'else'} path "
                      f"({len(branch)} step(s))")
            await run_steps(cog, ctx, branch, trace, budget)
            continue

        if step.type == "wait":
            seconds = int((step.config or {}).get("duration") or 0)
            if seconds > 0:
                raise Deferred(seconds, steps[index + 1:])
            continue

        spec = ACTIONS.get(step.type)
        if spec is None or spec.run is None:
            trace.add("error", step.type, False, "this action no longer exists")
            continue

        config = dict(step.config or {})
        config["_automation_id"] = ctx.variables.get("_automation_id")
        try:
            note = await spec.run(cog, ctx, config)
        except Stop:
            raise
        except Deferred:
            raise
        except Exception as e:
            # One failing action ends this automation's run but must not touch
            # the others queued for the same event.
            trace.add("error", spec.label, False, str(e))
            logger.info(f"Action {step.type} failed: {e}")
            return

        trace.add("action", spec.label, True, note or "done")
        if step.type == "stop":
            raise Stop()


# ---------------------------------------------------------------------- runs

async def run_automation(cog, automation: Automation, ctx: EventContext,
                         *, force: bool = False, record: bool = True) -> Tuple[str, Trace]:
    """Run one automation against one event. Returns (outcome, trace)."""
    trace = Trace()
    started = time.monotonic()
    ctx.variables["_automation_id"] = automation.id

    if automation.dry_run and not ctx.simulate:
        ctx.simulate = True
        trace.add("info", "Dry run", None,
                  "actions are logged but not performed")

    # The top-level gate. Checked before any step, and traced either way — a
    # run that stops here is exactly the case the history screen exists to
    # explain, so it is still recorded rather than dropped.
    if automation.conditions is not None and not automation.conditions.is_empty():
        if not await evaluate(cog, ctx, automation.conditions, trace):
            duration_ms = int((time.monotonic() - started) * 1000)
            if record:
                try:
                    await cog.db.record_run(
                        automation.id, "skipped",
                        f"{ctx.describe()} — {trace.summary()}",
                        duration_ms, trace.to_list())
                except Exception as e:
                    logger.warning(f"Could not record run for {automation.id}: {e}")
            return "skipped", trace

    outcome = "fired"
    try:
        budget = [MAX_ACTIONS_PER_RUN]
        await run_steps(cog, ctx, automation.steps, trace, budget)
    except Stop:
        trace.add("info", "Stop", None, "the automation stopped here")
    except Deferred as deferred:
        if ctx.simulate:
            trace.add("info", "Wait", None,
                      f"would wait {deferred.seconds}s, then continue")
        else:
            await _defer(cog, automation, ctx, deferred)
            trace.add("info", "Wait", None,
                      f"paused for {deferred.seconds}s; the rest is queued")
        outcome = "deferred"
    except Exception as e:
        logger.error(f"Automation {automation.name} crashed: {e}", exc_info=True)
        trace.add("error", "Automation", False, str(e))
        outcome = "error"

    if outcome == "fired" and ctx.simulate:
        outcome = "dry_run"

    duration_ms = int((time.monotonic() - started) * 1000)
    if record:
        try:
            await cog.db.record_run(automation.id, outcome,
                                    f"{ctx.describe()} — {trace.summary()}",
                                    duration_ms, trace.to_list())
            await cog.db.execute(
                "UPDATE automations SET last_fired_ts = ?, run_count = run_count + 1 "
                "WHERE id = ?", (now(), automation.id))
        except Exception as e:
            logger.warning(f"Could not record run for {automation.id}: {e}")
    return outcome, trace


async def _defer(cog, automation: Automation, ctx: EventContext, deferred: Deferred):
    """Persist the remainder of a run so a restart still finishes it."""
    import json
    payload = {
        "guild_id": ctx.guild_id,
        "user_id": ctx.user_id,
        "channel_id": ctx.channel_id,
        "message_id": ctx.message.id if ctx.message else 0,
        "event": ctx.event,
        # Only the JSON-safe scratch values survive; Discord objects are
        # re-resolved from the ids above when the step list resumes.
        "variables": {k: v for k, v in ctx.variables.items()
                      if isinstance(v, (str, int, float, bool, list, dict))},
    }
    await cog.db.execute(
        "INSERT INTO pending_actions (automation_id, execute_ts, steps, context, depth) "
        "VALUES (?, ?, ?, ?, ?)",
        (automation.id, now() + deferred.seconds,
         json.dumps([s.to_dict() for s in deferred.remaining]),
         json.dumps(payload), ctx.depth),
    )


async def run_by_id(cog, automation_id: str, ctx: EventContext) -> str:
    """The `run another automation` action. Enforces the depth cap."""
    if ctx.depth >= MAX_DEPTH:
        return f"not run — already {ctx.depth} automations deep"
    row = await cog.db.get_row("automations", automation_id)
    if row is None:
        return "not run — that automation no longer exists"
    automation = Automation.from_row(row)
    if not automation.enabled:
        return f"not run — {automation.name} is disabled"
    child = ctx.child()
    outcome, _ = await run_automation(cog, automation, child)
    return f"ran {automation.name} ({outcome})"


# ------------------------------------------------------------------ dispatch

async def handle(cog, event: str, ctx: EventContext) -> int:
    """Entry point from the cog's listeners. Returns how many automations ran."""
    if not await cog.automations_enabled():
        return 0

    automations = cog.automations_for(event)
    if not automations:
        return 0

    ran = 0
    for automation in automations:
        if not automation.enabled:
            continue
        if ctx.is_bot_actor() and not automation.allow_bots:
            continue
        if not _trigger_config_matches(automation, ctx):
            continue
        if not _budget_ok(automation.id):
            logger.warning(
                f"Automation {automation.name} hit its rate budget "
                f"({MAX_RUNS_PER_MINUTE}/min) and was skipped.")
            continue
        if automation.cooldown_s:
            key = ctx.cooldown_key(automation.cooldown_scope)
            if await cog.db.cooldown_active(automation.id, key):
                continue

        # Each automation gets its own context: one setting a variable or
        # storing regex captures must not leak into the next.
        run_ctx = ctx.child(depth_increment=0)
        outcome, _ = await run_automation(cog, automation, run_ctx)
        ran += 1

        if automation.cooldown_s and outcome in ("fired", "deferred"):
            await cog.db.cooldown_set(
                automation.id, ctx.cooldown_key(automation.cooldown_scope),
                automation.cooldown_s)
        if automation.stop_after and outcome == "fired":
            break
    return ran


def _trigger_config_matches(automation: Automation, ctx: EventContext) -> bool:
    """Trigger-level filtering, before any condition runs.

    Only the role triggers have this today: firing on "any role gained" and
    then filtering in a condition works, but it costs a run and a log entry
    for every role change in the server.
    """
    config = automation.trigger_config or {}
    if automation.trigger_type in ("role_added", "role_removed"):
        wanted = {int(r) for r in (config.get("roles") or []) if str(r).isdigit()}
        if wanted:
            role_id = ctx.extra.get("role_id")
            return role_id in wanted
    return True


async def explain(cog, automation: Automation, ctx: EventContext) -> Trace:
    """Run against a real event with every side effect suppressed.

    This is what powers "why didn't it fire?" and the Test button: the same
    code path as a real run, so the answer it gives is the truth rather than a
    reimplementation that can drift.
    """
    ctx.simulate = True
    _, trace = await run_automation(cog, automation, ctx, record=False)
    return trace
