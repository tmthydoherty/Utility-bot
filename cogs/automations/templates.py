"""Ready-made automations.

Starting from a blank automation means understanding the whole model before
you can build anything — which trigger, which checks, which actions, in what
order. Starting from one that already works means reading something concrete
and changing the parts you disagree with, which is a much shorter path to
understanding.

Each template lands fully wired and switched **off in test mode**. Nothing here
can act until it is explicitly turned on.

What is left to decide comes from two places, and the split matters:

    `readiness` derives the blanks that stop it working — no channel picked, no
    role chosen. Those are the same whether the automation came from a template
    or was built by hand, so no template repeats them.

    `asks` names the settings only the template's author knows are worth a
    decision, by field path so the setup flow can open each one. See `Ask`.

Prose in `needs` is the remainder: advice that points at no single setting.
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from typing import Any, Dict, List


@dataclass
class Ask:
    """A setting worth a decision that nothing can work out on its own.

    `readiness` derives the blanks that stop an automation working, and that
    covers most of what a template leaves open. What it cannot know is intent:
    `ticket_alert` posts perfectly happily into the ticket it was told about,
    because a ticket supplies its own channel — it is simply not the *staff*
    channel the template is for, and no rule can see the difference.

    So a template names those settings itself, and names them by path rather
    than in prose, which is the whole point: `("action", [0], "channel")`
    resolves through `find_step_in`, so the setup flow can open the picker for
    it instead of describing where to go and hoping.

    Where each one surfaces depends on the value that is there now, not on
    another flag to keep in step:

        still empty  — asked during setup, skippable, never blocks Turn on.
        already set  — offered on the last screen as worth a look, since a
                       filled-in default is a choice someone might want to
                       revisit but nobody needs to be stopped for.
    """

    where: str                       # "trigger" | "condition" | "action"
    path: List[int]                  # [] for the trigger
    field: str                       # the field key
    prompt: str                      # the question, in the reader's language


def _ask(where: str, path, field: str, prompt: str) -> Ask:
    return Ask(where, list(path), field, prompt)


@dataclass
class Template:
    key: str
    name: str
    blurb: str                       # one line, plain language
    category: str = "Popular"
    trigger_type: str = "message_sent"
    trigger_config: Dict[str, Any] = dc_field(default_factory=dict)
    conditions: Dict[str, Any] = dc_field(default_factory=dict)
    steps: List[Dict[str, Any]] = dc_field(default_factory=list)
    # Settings this template wants a decision about, addressed by path. See
    # `Ask` — these are the ones no rule can derive.
    asks: List[Ask] = dc_field(default_factory=list)
    # Advice that points at no single setting: "make a second copy for the
    # morning", "consider exempting staff". Shown as prose at the end, because
    # there is nothing to open.
    #
    # Anything that *does* point at a setting belongs in `asks`, and anything
    # `readiness` already derives belongs in neither — repeating a question the
    # flow just asked is how a checklist starts getting skimmed.
    needs: List[str] = dc_field(default_factory=list)


def _all(*items) -> dict:
    return {"op": "and", "items": list(items)}


def _check(kind: str, **config) -> dict:
    return {"type": kind, "config": config}


def _check_not(kind: str, **config) -> dict:
    """A check that has to *not* match — the same as setting
    'This check should — Not match' in the panel."""
    return {"type": kind, "config": config, "negate": True}


def _do(kind: str, **config) -> dict:
    return {"type": kind, "config": config}


TEMPLATES: List[Template] = [
    Template(
        key="welcome",
        name="Welcome new members",
        blurb="Post a greeting in a channel whenever someone joins.",
        trigger_type="member_joined",
        steps=[_do("send_message", destination="channel",
                   content="Welcome to {guild.name}, {user.mention}! 👋")],
        # The channel is a derived blank, so the flow already asks for it.
        needs=[],
    ),
    Template(
        key="keyword_reply",
        name="Reply to a word or phrase",
        blurb="When someone says something, reply to their message.",
        conditions=_all(
            _check_not("is_reply"),
            _check("content_contains", text="hello, hi, hey"),
        ),
        steps=[_do("send_message", destination="reply",
                   content="Hey {user.name}! 👋")],
        asks=[_ask("condition", [1], "text",
                   "Which words or phrases should this reply to?"),
              _ask("action", [0], "content", "What should the bot say back?")],
    ),
    Template(
        key="react_to_images",
        name="React to every image",
        blurb="Add reactions to any picture or clip posted in a channel.",
        conditions=_all(
            _check("channel_is", channels=[]),
            _check("has_image"),
        ),
        steps=[_do("add_reaction", emoji=["⬆️", "⬇️"])],
        asks=[_ask("action", [0], "emoji",
                   "Which reactions should it add?")],
    ),
    Template(
        key="auto_thread",
        name="Start a thread on every post",
        blurb="Opens a comment thread under each message in a channel.",
        conditions=_all(_check("channel_is", channels=[])),
        steps=[_do("create_thread", name="{user} — discussion",
                   archive_minutes=1440)],
        needs=[],
    ),
    Template(
        key="no_links",
        name="No links in this channel",
        blurb="Deletes messages containing links, and tells the person why.",
        category="Moderation",
        conditions=_all(
            _check("channel_is", channels=[]),
            _check("has_link"),
        ),
        steps=[
            _do("send_message", destination="dm",
                content="Heads up — links aren't allowed in that channel."),
            _do("delete_message"),
        ],
        needs=["Consider adding a check so staff aren't caught by it — add "
               "\"They have a permission\" set to Manage Messages, then switch "
               "it to 'Not match'"],
    ),
    Template(
        key="calm_the_caps",
        name="Ask people not to shout",
        blurb="Replies to messages that are mostly capital letters.",
        category="Moderation",
        conditions=_all(_check("caps_ratio", percent=70, min_length=10)),
        steps=[_do("send_message", destination="reply",
                   content="No need to shout! 🙂")],
        asks=[_ask("condition", [0], "percent",
                   "How much of the message has to be capitals to count?")],
    ),
    Template(
        key="three_strikes",
        name="Three strikes, then a timeout",
        blurb=("Counts how often someone breaks a rule and times them out on "
               "the third time within a day."),
        category="Moderation",
        conditions=_all(_check("content_contains", text="badword")),
        steps=[
            _do("counter_change", key="strikes", scope="user", amount=1,
                window=86400),
            _do("delete_message"),
            {
                "type": "if",
                "conditions": _all(
                    _check("counter_compare", key="strikes", scope="user",
                           op="gte", value=3)),
                "then": [
                    _do("timeout_member", duration=600,
                        reason="Three strikes in one day"),
                    _do("send_message", destination="dm",
                        content=("That's 3 strikes today, so you're timed out "
                                 "for 10 minutes.")),
                ],
                "else": [
                    _do("send_message", destination="dm",
                        content="That's strike {counter.strikes} of 3 today."),
                ],
            },
        ],
        asks=[_ask("condition", [0], "text",
                   "Which words should count as a strike?"),
              _ask("action", [2, 0, 0], "duration",
                   "How long should the timeout last?")],
    ),
    Template(
        key="role_on_keyword",
        name="Give a role when someone asks",
        blurb="Someone types a word and gets a role automatically.",
        category="Roles",
        conditions=_all(_check("content_exactly", text="!pings")),
        steps=[
            _do("toggle_role", role=0),
            _do("add_reaction", emoji=["✅"]),
        ],
        asks=[_ask("condition", [0], "text",
                   "What should people type to get the role?")],
    ),
    Template(
        key="temporary_role",
        name="Give a role for an hour",
        blurb="Hands out a role that takes itself back later.",
        category="Roles",
        conditions=_all(_check("content_exactly", text="!vip")),
        steps=[_do("add_role", role=0, duration=3600)],
        asks=[_ask("condition", [0], "text",
                   "What should people type to get the role?"),
              _ask("action", [0], "duration", "How long should they keep it?")],
    ),
    Template(
        key="role_welcome_pack",
        name="Big welcome when someone gets a role",
        blurb=("DMs them a picture embed, announces it in a channel, and reacts "
               "to that announcement."),
        category="Roles",
        trigger_type="role_added",
        trigger_config={"roles": []},
        steps=[
            _do("send_message",
                destination="dm",
                use_embed=True,
                embed_title="Welcome aboard!",
                content="You've just been given a new role in {guild.name}. 🎉",
                embed_colour="green",
                embed_thumbnail="{user.avatar}",
                embed_image="",
                embed_footer="{guild.name}"),
            _do("send_message",
                destination="channel",
                channel=0,
                content="Everyone welcome {user.mention}! 🎉"),
            # Reacts to the announcement above, not to any incoming message —
            # a role change has no message of its own.
            _do("add_reaction", emoji=["🎉", "👋", "❤️"], target="sent"),
        ],
        asks=[_ask("trigger", [], "roles",
                   "Which role should set this off? Leaving it empty runs for "
                   "any role at all"),
              _ask("action", [0], "embed_image",
                   "A GIF or image for the DM, if you want one"),
              _ask("action", [2], "emoji",
                   "Which emoji should it react with?")],
    ),
    Template(
        key="goodbye",
        name="Say goodbye when someone leaves",
        blurb="Posts a note in a channel when a member leaves the server.",
        trigger_type="member_left",
        steps=[_do("send_message", destination="channel",
                   content="{user.name} has left the server.")],
        needs=[],
    ),
    Template(
        key="level_up",
        name="Congratulate a level up",
        blurb="Celebrates when someone reaches a new level.",
        category="Rewards",
        trigger_type="member_level_up",
        steps=[_do("send_message", destination="channel",
                   content="🎉 {user.mention} just levelled up!")],
        needs=[],
    ),
    Template(
        key="thank_booster",
        name="Thank a new booster",
        blurb="Says thanks and gives a role when someone boosts the server.",
        category="Rewards",
        trigger_type="member_boosted",
        steps=[
            _do("send_message", destination="channel",
                content="💜 Thank you for boosting, {user.mention}!"),
            _do("add_role", role=0),
        ],
        needs=[],
    ),
    Template(
        key="ticket_alert",
        name="Alert staff about new tickets",
        blurb="Posts in a staff channel whenever a ticket is opened.",
        category="Staff",
        trigger_type="ticket_opened",
        steps=[_do("send_message", destination="channel",
                   content="🎫 {user.mention} opened a ticket.")],
        # A ticket supplies its own channel, so nothing here is *blank* —
        # left alone the alert posts into the ticket, where no one is watching.
        asks=[_ask("action", [0], "channel",
                   "Which staff channel should the alert go to?")],
    ),
    Template(
        key="quiet_hours",
        name="Slow the chat down overnight",
        blurb="Turns slowmode on at night and off again in the morning.",
        category="Staff",
        trigger_type="schedule",
        trigger_config={"frequency": "daily", "time_utc": "02:00"},
        steps=[_do("set_slowmode", channel=0, seconds=30)],
        asks=[_ask("trigger", [], "time_utc",
                   "What time should the chat slow down? (UTC, 24-hour)"),
              _ask("action", [0], "seconds",
                   "How many seconds between messages?")],
        needs=["Make a second copy set to the morning, with slowmode back to "
               "0, to turn it off again"],
    ),
]

BY_KEY = {t.key: t for t in TEMPLATES}


def categories() -> Dict[str, List[Template]]:
    grouped: Dict[str, List[Template]] = {}
    for template in TEMPLATES:
        grouped.setdefault(template.category, []).append(template)
    return grouped
