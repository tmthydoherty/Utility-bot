"""The shape of an automation.

An automation is a trigger plus a list of steps. A step is either an action or
an `if`, and an `if` holds two more step lists. That one recursive shape gives
branching and arbitrary nesting without a node-graph editor, and composition
comes free because "run another automation" is just another action.

Conditions are a tree of their own: `and`/`or` groups holding leaves or more
groups, each of which may be negated. MEE6's automations are a flat list of
conditions joined by a single implicit AND, which cannot express
"(has the role OR is level 10) AND is not in #staff" — the rule people
actually want most of the time.

Everything here is plain data: it parses from JSON, serialises back, and knows
nothing about Discord or the database. That is what makes the engine testable
without a bot.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field as dc_field
from typing import Any, Dict, List, Optional, Union

MAX_DEPTH = 8          # guards a hand-edited graph from recursing forever
MAX_STEPS = 50         # per automation, counted across the whole tree


# ---------------------------------------------------------------- conditions

@dataclass
class ConditionLeaf:
    type: str
    config: Dict[str, Any] = dc_field(default_factory=dict)
    negate: bool = False

    def to_dict(self) -> dict:
        data = {"type": self.type, "config": self.config}
        if self.negate:
            data["negate"] = True
        return data


@dataclass
class ConditionGroup:
    op: str = "and"                     # and | or
    items: List[Union["ConditionGroup", ConditionLeaf]] = dc_field(default_factory=list)
    negate: bool = False

    def to_dict(self) -> dict:
        data = {"op": self.op, "items": [i.to_dict() for i in self.items]}
        if self.negate:
            data["negate"] = True
        return data

    def is_empty(self) -> bool:
        """An empty group matches everything — worth saying out loud in the UI."""
        return not self.items


def parse_condition(data: Any, depth: int = 0) -> Union[ConditionGroup, ConditionLeaf]:
    if depth > MAX_DEPTH or not isinstance(data, dict):
        return ConditionGroup()
    if "op" in data:
        return ConditionGroup(
            op="or" if data.get("op") == "or" else "and",
            items=[parse_condition(i, depth + 1) for i in data.get("items", [])
                   if _is_real_condition(i)],
            negate=bool(data.get("negate")),
        )
    if not data.get("type"):
        # `{}` is what an automation with no conditions stores, and a leaf with
        # no type is not a condition — it is the absence of one. Returning a
        # leaf here put a nameless entry in the tree, which rendered as a
        # blank select option and made Discord reject the whole menu.
        return ConditionGroup()
    return ConditionLeaf(
        type=str(data.get("type")),
        config=data.get("config") or {},
        negate=bool(data.get("negate")),
    )


def _is_real_condition(data: Any) -> bool:
    """Filters typeless leftovers out of a group's children."""
    return isinstance(data, dict) and bool(data.get("op") or data.get("type"))


# --------------------------------------------------------------------- steps

@dataclass
class Step:
    type: str                            # an action key, or "if"
    config: Dict[str, Any] = dc_field(default_factory=dict)
    conditions: Optional[ConditionGroup] = None      # only for "if"
    then: List["Step"] = dc_field(default_factory=list)
    otherwise: List["Step"] = dc_field(default_factory=list)

    @property
    def is_branch(self) -> bool:
        return self.type == "if"

    def to_dict(self) -> dict:
        if self.is_branch:
            return {
                "type": "if",
                "conditions": (self.conditions or ConditionGroup()).to_dict(),
                "then": [s.to_dict() for s in self.then],
                "else": [s.to_dict() for s in self.otherwise],
            }
        return {"type": self.type, "config": self.config}

    def count(self) -> int:
        return 1 + sum(s.count() for s in self.then) + sum(s.count() for s in self.otherwise)


def parse_step(data: Any, depth: int = 0) -> Optional[Step]:
    if depth > MAX_DEPTH or not isinstance(data, dict):
        return None
    step_type = str(data.get("type", ""))
    if not step_type:
        return None
    if step_type == "if":
        return Step(
            type="if",
            conditions=parse_condition(data.get("conditions") or {}),
            then=parse_steps(data.get("then"), depth + 1),
            otherwise=parse_steps(data.get("else"), depth + 1),
        )
    return Step(type=step_type, config=data.get("config") or {})


def parse_steps(data: Any, depth: int = 0) -> List[Step]:
    if not isinstance(data, list):
        return []
    steps = []
    for entry in data:
        step = parse_step(entry, depth)
        if step is not None:
            steps.append(step)
    return steps


def find_condition(root: ConditionGroup,
                   path: List[int]) -> Optional[Union[ConditionGroup, ConditionLeaf]]:
    """Address a condition node by its position in the tree."""
    node: Any = root
    for index in path:
        if not isinstance(node, ConditionGroup):
            return None
        if index < 0 or index >= len(node.items):
            return None
        node = node.items[index]
    return node


def remove_condition(root: ConditionGroup, path: List[int]) -> bool:
    """Drop the node at `path`. The root itself cannot be removed."""
    if not path:
        return False
    parent = find_condition(root, path[:-1])
    if not isinstance(parent, ConditionGroup):
        return False
    index = path[-1]
    if index < 0 or index >= len(parent.items):
        return False
    parent.items.pop(index)
    return True


def walk_conditions(node, path: List[int] = None, depth: int = 0):
    """Yield `(path, node, depth)` for every node, parents before children."""
    path = path or []
    yield path, node, depth
    if isinstance(node, ConditionGroup):
        for index, child in enumerate(node.items):
            yield from walk_conditions(child, path + [index], depth + 1)


def walk_steps(steps: List[Step], path: List[int] = None, depth: int = 0):
    """Yield `(path, step, depth)` for a step list, descending into branches."""
    path = path or []
    for index, step in enumerate(steps):
        here = path + [index]
        yield here, step, depth
        if step.is_branch:
            for child in walk_steps(step.then, here + [0], depth + 1):
                yield child
            for child in walk_steps(step.otherwise, here + [1], depth + 1):
                yield child


def find_step_in(steps: List[Step], path: List[int]) -> Optional[Step]:
    """Resolve a `walk_steps` path, which alternates index and branch side."""
    current = steps
    step = None
    position = 0
    while position < len(path):
        index = path[position]
        if index < 0 or index >= len(current):
            return None
        step = current[index]
        position += 1
        if position < len(path):
            if not step.is_branch:
                return None
            side = path[position]
            current = step.then if side == 0 else step.otherwise
            position += 1
    return step


def remove_step(steps: List[Step], path: List[int]) -> bool:
    """Drop the step at a `walk_steps` path."""
    if not path:
        return False
    current = steps
    position = 0
    while position < len(path) - 1:
        index = path[position]
        if index < 0 or index >= len(current):
            return False
        step = current[index]
        position += 1
        if not step.is_branch:
            return False
        current = step.then if path[position] == 0 else step.otherwise
        position += 1
    index = path[-1]
    if index < 0 or index >= len(current):
        return False
    current.pop(index)
    return True


def container_for(steps: List[Step], path: List[int]) -> Optional[List[Step]]:
    """The list a `walk_steps` path lives in, for appending or reordering."""
    current = steps
    position = 0
    while position < len(path) - 1:
        index = path[position]
        if index < 0 or index >= len(current):
            return None
        step = current[index]
        position += 1
        if not step.is_branch:
            return None
        current = step.then if path[position] == 0 else step.otherwise
        position += 1
    return current


def find_step(steps: List[Step], path: List[int]) -> Optional[Step]:
    """Address a step by its position, e.g. [0, 1, 2] = step 0's `then` branch.

    The builder needs stable addressing to edit a nested step, and a path is
    what a select option can carry in 100 characters.
    """
    current = steps
    step = None
    for index in path:
        # Negative indices would silently wrap to the end of the list.
        if index < 0 or index >= len(current):
            return None
        step = current[index]
        current = step.then if step.is_branch else []
    return step


# --------------------------------------------------------------- automation

def _column(row, name: str, default=None):
    """Read a column that may not exist on an older row.

    sqlite3.Row raises IndexError rather than returning None for an unknown
    key, so a plain `row["conditions"]` would break every automation written
    before that column existed.
    """
    try:
        return row[name]
    except (IndexError, KeyError, TypeError):
        return default


@dataclass
class Automation:
    id: str
    name: str
    trigger_type: str
    trigger_config: Dict[str, Any] = dc_field(default_factory=dict)
    # The top-level gate, checked before any step runs.
    conditions: ConditionGroup = dc_field(default_factory=ConditionGroup)
    steps: List[Step] = dc_field(default_factory=list)
    description: str = ""
    enabled: bool = False
    dry_run: bool = True
    priority: int = 100
    stop_after: bool = False
    cooldown_s: int = 0
    cooldown_scope: str = "user"
    allow_bots: bool = False
    guild_id: int = 0
    version: int = 1
    # Which ready-made automation this started from, so the setup flow can find
    # that template's own questions later. Empty when built from scratch, and
    # never consulted by the engine.
    template_key: str = ""

    @classmethod
    def from_row(cls, row) -> "Automation":
        graph = row["graph"]
        try:
            parsed = json.loads(graph) if isinstance(graph, str) else (graph or {})
        except (json.JSONDecodeError, TypeError):
            parsed = {}
        try:
            trigger_config = json.loads(row["trigger_config"] or "{}")
        except (json.JSONDecodeError, TypeError):
            trigger_config = {}
        # `conditions` arrived after the first release, so a row written by the
        # older build has no such key at all.
        try:
            raw_conditions = json.loads(_column(row, "conditions") or "{}")
        except (json.JSONDecodeError, TypeError):
            raw_conditions = {}
        conditions = parse_condition(raw_conditions)
        if not isinstance(conditions, ConditionGroup):
            conditions = ConditionGroup(items=[conditions])
        return cls(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            trigger_type=row["trigger_type"],
            trigger_config=trigger_config if isinstance(trigger_config, dict) else {},
            conditions=conditions,
            steps=parse_steps(parsed.get("steps")),
            enabled=bool(row["enabled"]),
            dry_run=bool(row["dry_run"]),
            priority=row["priority"],
            stop_after=bool(row["stop_after"]),
            cooldown_s=row["cooldown_s"],
            cooldown_scope=row["cooldown_scope"],
            allow_bots=bool(row["allow_bots"]),
            guild_id=row["guild_id"],
            version=row["version"],
            # Added after the first release, so a row written before it has no
            # such column at all.
            template_key=_column(row, "template_key") or "",
        )

    def graph_json(self) -> str:
        return json.dumps({"steps": [s.to_dict() for s in self.steps]})

    def conditions_json(self) -> str:
        return json.dumps(self.conditions.to_dict())

    def step_count(self) -> int:
        return sum(s.count() for s in self.steps)
