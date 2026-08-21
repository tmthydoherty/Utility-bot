"""Turning a stored response into the text that gets sent.

A response is a template. Two layers run over it:

* **Placeholders** — ``{user}``, ``{server}``, ``{count}`` and friends: facts
  about who ran the command and where.
* **Scripting** — ``{random:a|b|c}`` / ``{choose:a|b|c}`` picks one option, and
  ``{math:1+2*3}`` evaluates a small arithmetic expression. These let one
  command feel different every time without needing several rows.

Everything here is deliberately small and total: an unknown token is left as
written rather than raising, because a template is admin-authored content that
should never be able to make a command throw at message time.

The one place that could misbehave is the arithmetic, so it does not touch
``eval``. It walks a parsed AST and permits only numbers and the basic binary
operators — anything else (a name, a call, an attribute) yields the literal
text back untouched.
"""

from __future__ import annotations

import ast
import operator
import random
import re
import typing

import discord

# {token} or {token:argument}. The argument runs to the closing brace and may
# itself contain '|' separators; it may not contain a nested brace, which keeps
# the matcher simple and is all the feature needs.
_TOKEN = re.compile(r"\{([a-zA-Z_]+)(?::([^{}]*))?\}")

_MATH_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}


def _eval_math(expr: str) -> typing.Optional[str]:
    """Evaluate a bare arithmetic expression, or ``None`` if it isn't one.

    Only numbers and the operators in ``_MATH_OPS`` are allowed; the power
    operator is capped so ``{math:9**999999}`` can't wedge the event loop. A
    ``None`` return lets the caller leave the whole ``{math:…}`` token in place,
    so a typo shows up as itself rather than as half-evaluated text.
    """
    try:
        node = ast.parse(expr, mode="eval").body
        value = _eval_node(node)
    except (SyntaxError, ValueError, TypeError, ZeroDivisionError, OverflowError):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value)


def _eval_node(node):
    if isinstance(node, ast.Constant):
        if isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
            return node.value
        raise ValueError("only numbers are allowed")
    if isinstance(node, ast.BinOp) and type(node.op) in _MATH_OPS:
        if isinstance(node.op, ast.Pow):
            exponent = _eval_node(node.right)
            if abs(exponent) > 64:
                raise ValueError("exponent too large")
        return _MATH_OPS[type(node.op)](_eval_node(node.left), _eval_node(node.right))
    if isinstance(node, ast.UnaryOp) and type(node.op) in _MATH_OPS:
        return _MATH_OPS[type(node.op)](_eval_node(node.operand))
    raise ValueError("unsupported expression")


def substitute(text: str, message: discord.Message, use_count: int) -> str:
    """Expand every token in a response template.

    ``use_count`` is the total already recorded for this command, so ``{count}``
    reads as the number of the *current* use.
    """
    guild = message.guild
    author = message.author
    channel = message.channel

    def replace(match: re.Match) -> str:
        token = match.group(1).lower()
        arg = match.group(2)

        if token in ("random", "choose") and arg is not None:
            options = [part.strip() for part in arg.split("|") if part.strip()]
            return random.choice(options) if options else ""
        if token == "math" and arg is not None:
            result = _eval_math(arg.strip())
            return result if result is not None else match.group(0)

        if token == "user":
            return author.mention
        if token == "username":
            return author.display_name
        if token in ("server", "guild"):
            return guild.name if guild else ""
        if token == "channel":
            return getattr(channel, "mention", "")
        if token in ("membercount", "members"):
            return str(guild.member_count) if guild and guild.member_count else "0"
        if token == "count":
            return str(use_count + 1)

        # Unknown token: leave it exactly as the admin wrote it.
        return match.group(0)

    return _TOKEN.sub(replace, text)


def pick_response(responses: list[str]) -> str:
    """One of the command's responses, chosen at random. Empty if it has none."""
    pool = [r for r in responses if isinstance(r, str) and r.strip()]
    return random.choice(pool) if pool else ""
