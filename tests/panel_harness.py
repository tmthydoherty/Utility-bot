"""A fake Discord, and the structural checks every panel has to pass.

The panel is a tree of pages that all draw onto a single ephemeral message, so
what can silently break is structural: a row overflowing Discord's
five-component limit, a select past its 25-option cap, a page losing its Back
button after a rebuild. None of that needs a real gateway connection to catch,
and all of it needs the same stand-ins — so they live here rather than being
written twice, once per cog.

`check` is injected by the suite that imports this, so a failure is recorded
against the right run.
"""
import ast
from pathlib import Path

import discord

from utils.panel import MAX_SELECT_OPTIONS

# Set by `bind(...)` below. A module-level hook rather than threading a
# recorder through every helper: these are assertions, and an assertion that
# has to be handed its own reporter reads worse than one that just fails.
check = None


def bind(recorder):
    """Point the helpers at the importing suite's own `check`."""
    global check
    check = recorder


# --------------------------------------------------------------- fake discord

class FakeChannel:
    def __init__(self, cid, name="general"):
        self.id, self.name = cid, name


class FakeRole:
    def __init__(self, rid):
        self.id, self.name = rid, f"Role{rid}"


class FakeGuild:
    id = 1

    def get_channel(self, cid):
        return FakeChannel(cid, f"chan{cid}")

    def get_role(self, rid):
        return FakeRole(rid)


class FakeResponse:
    def __init__(self, parent):
        self.parent = parent

    def is_done(self):
        return False

    async def edit_message(self, **kwargs):
        self.parent.edits.append(kwargs)

    async def send_message(self, *args, **kwargs):
        self.parent.sends.append((args, kwargs))

    async def send_modal(self, modal):
        self.parent.modals.append(modal)


class FakeUser:
    """Shaped like a real member, so placeholder rendering behaves as it will."""
    id = 42
    name = "admin"
    display_name = "Admin"
    mention = "<@42>"
    bot = False
    roles = ()

    def __str__(self):
        return self.name


class FakeInteraction:
    def __init__(self):
        self.guild = FakeGuild()
        self.guild_id = 1
        self.user = FakeUser()
        self.channel = FakeChannel(500, "panel-channel")
        self.message = None
        self.response = FakeResponse(self)
        self.edits, self.sends, self.modals = [], [], []
        self.data = {}


class FakeBot:
    def get_cog(self, name):
        return None

    def get_guild(self, gid):
        return None


class FakeCog:
    def __init__(self, db):
        self.db = db
        self.bot = FakeBot()

    def is_admin(self, user):
        return True

    async def refresh_cache(self):
        pass

    async def save_made(self):
        pass

    async def automations_enabled(self):
        return True


# ------------------------------------------------------------------ structure

def walk_layout(view, label):
    """Discord allows 5 action rows of 5 components; a select fills its row."""
    rows = {}
    for item in view.children:
        rows.setdefault(item.row if item.row is not None else 0, []).append(item)
    for row, items in rows.items():
        selects = [i for i in items if isinstance(i, discord.ui.Select)]
        if selects:
            check(len(items) == 1,
                  f"{label}: row {row} holds a select alone ({len(items)} items)")
        else:
            check(len(items) <= 5,
                  f"{label}: row {row} within the 5-component limit ({len(items)})")
        for item in items:
            if isinstance(item, discord.ui.Select) and item.options:
                check(len(item.options) <= MAX_SELECT_OPTIONS,
                      f"{label}: select has {len(item.options)} options (cap 25)")
                # Discord rejects the *entire* menu over one bad option, with a
                # 400 that surfaces to the user as "the bot didn't respond".
                # These are its actual limits.
                for i, option in enumerate(item.options):
                    check(1 <= len(option.label or "") <= 100,
                          f"{label}: option {i} label is 1-100 chars "
                          f"(got {len(option.label or '')}: {option.label!r})")
                    check(1 <= len(option.value or "") <= 100,
                          f"{label}: option {i} value is 1-100 chars "
                          f"({option.value!r})")
                    description = option.description
                    check(description is None or 1 <= len(description) <= 100,
                          f"{label}: option {i} description is 1-100 chars "
                          f"({description!r})")
                values = [o.value for o in item.options]
                check(len(values) == len(set(values)),
                      f"{label}: option values are unique")
                if item.placeholder is not None:
                    check(len(item.placeholder) <= 150,
                          f"{label}: placeholder within 150 chars")
    check(len(rows) <= 5, f"{label}: at most 5 rows ({len(rows)})")


def has_back(view) -> bool:
    from utils.panel import BackButton
    return any(isinstance(i, BackButton) for i in view.children)



def audit_send_message(roots):
    """No page in the tree may call send_message outside the sanctioned spots.

    Allowed: PanelPage.open (the entry point), interaction_check (a denial has
    to be its own message), respond_to_modal (fallback when there is no
    message to repaint), and send_modal, which is not a message at all.
    """
    allowed_functions = {"open", "interaction_check", "respond_to_modal",
                         "_on_timeout", "on_timeout"}
    offenders = []
    files = []
    for root in roots:
        root = Path(root)
        files.extend([root] if root.is_file() else sorted(root.rglob("*.py")))

    for path in files:
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name in allowed_functions:
                continue
            for inner in ast.walk(node):
                if (isinstance(inner, ast.Attribute)
                        and inner.attr == "send_message"):
                    offenders.append(f"{path}:{inner.lineno} in {node.name}()")
    check(not offenders,
          f"no stray send_message in the page tree ({offenders or 'clean'})")
