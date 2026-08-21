"""The catalogue of everything an automation can watch for, check and do.

Every entry here is one declarative record. It declares its options with
`Field` from `schema.py`, and the panel builds a settings screen from that
description generically — so adding an action is this entry plus one async
function in `actions.py`, with no UI work at all.

**Wording matters as much as the code.** Every label is written to complete
the sentence its screen asks, so an automation reads as English:

    When this happens →  *someone sends a message*
    Only if           →  *the message contains certain words*
    Then do this      →  *reply to the message*

Nothing here says "trigger", "condition", "predicate" or "node". If a label
needs a manual to understand, it is the wrong label.

`perms` lists the Discord permissions an action needs, which the builder
checks up front so a missing permission is a warning on the screen rather than
a silent failure at 3am.
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from typing import Any, Callable, Dict, List, Optional

from utils.fieldspec import Field, FieldType
from . import actions as action_impl
from . import conditions as condition_impl
from utils.embed_builder import embed_fields


def _which_message() -> Field:
    """Lets an action work on the bot's own message instead of the trigger.

    Without this an automation can only ever touch the message that set it
    off, so "announce something, then react to the announcement" is
    impossible — and on a trigger like gaining a role there is no message at
    all.
    """
    return Field(
        "target", "Which message", FieldType.CHOICE,
        "Pick the one I sent if an earlier step posted it.",
        choices=[("trigger", "The message that set this off"),
                 ("sent", "The message I sent earlier in this automation")],
        default="trigger")


@dataclass
class TriggerSpec:
    key: str
    label: str
    description: str
    category: str = "Messages"
    fields: List[Field] = dc_field(default_factory=list)
    provides: tuple = ()
    # True when this arrives over `bot.dispatch` from another cog rather than
    # from Discord directly. Those need a matching `on_<key>` listener in
    # cog.py, which is checked by tests/utility/check_dispatch.py. Stated
    # explicitly rather than inferred from `category`, because the category is
    # a display grouping and gets reshuffled for readability.
    dispatched: bool = False


@dataclass
class ConditionSpec:
    key: str
    label: str
    description: str
    category: str = "Other"
    fields: List[Field] = dc_field(default_factory=list)
    run: Optional[Callable] = None
    requires: tuple = ()


@dataclass
class ActionSpec:
    key: str
    label: str
    description: str
    category: str = "Other"
    fields: List[Field] = dc_field(default_factory=list)
    run: Optional[Callable] = None
    perms: tuple = ()
    requires: tuple = ()
    destructive: bool = False


# ------------------------------------------------- when this happens
# Labels complete the sentence "Run this automation when…".

TRIGGERS: Dict[str, TriggerSpec] = {t.key: t for t in [
    TriggerSpec("message_sent", "Someone sends a message",
                "Runs on every new message in the server.",
                category="Messages",
                provides=("message", "channel", "member")),
    TriggerSpec("message_edited", "Someone edits a message",
                "Runs when a message is changed after being sent.",
                category="Messages",
                provides=("message", "channel", "member")),
    TriggerSpec("message_deleted", "A message is deleted",
                "Runs when a message is removed.",
                category="Messages",
                provides=("message", "channel", "member")),
    TriggerSpec("reaction_added", "Someone reacts to a message",
                "Runs when a reaction is added.",
                category="Messages",
                provides=("message", "channel", "member")),

    TriggerSpec("member_joined", "Someone joins the server",
                "Runs when a new member arrives.",
                category="Members", provides=("member",)),
    TriggerSpec("member_left", "Someone leaves the server",
                "Runs when a member leaves, is kicked, or is banned.",
                category="Members", provides=("member",)),
    TriggerSpec("member_boosted", "Someone boosts the server",
                "Runs when a member starts boosting.",
                category="Members", provides=("member",)),
    TriggerSpec("nickname_changed", "Someone changes their nickname",
                "Runs when a member renames themselves here.",
                category="Members", provides=("member",)),
    TriggerSpec("role_added", "Someone gets a role",
                "Runs when a member gains a role.",
                category="Members",
                fields=[Field("roles", "Which roles", FieldType.ROLE,
                              "Leave this empty to run for any role.",
                              multi=True)],
                provides=("member",)),
    TriggerSpec("role_removed", "Someone loses a role",
                "Runs when a member loses a role.",
                category="Members",
                fields=[Field("roles", "Which roles", FieldType.ROLE,
                              "Leave this empty to run for any role.",
                              multi=True)],
                provides=("member",)),

    TriggerSpec("voice_joined", "Someone joins a voice channel",
                "Runs when a member connects to voice.",
                category="Voice", provides=("member", "channel")),
    TriggerSpec("voice_left", "Someone leaves a voice channel",
                "Runs when a member disconnects from voice.",
                category="Voice", provides=("member", "channel")),
    TriggerSpec("vc_lobby_created", "A voice lobby is created",
                "Runs when someone opens one of the temporary voice lobbies.",
                category="Voice", provides=("member", "channel"), dispatched=True),
    TriggerSpec("vc_lobby_closed", "A voice lobby closes",
                "Runs when a temporary voice lobby is cleaned up.",
                category="Voice", provides=("member",), dispatched=True),

    TriggerSpec("member_level_up", "Someone levels up",
                "Runs when a member reaches a new level.",
                category="Levels & tickets", provides=("member",), dispatched=True),
    TriggerSpec("ticket_opened", "Someone opens a ticket",
                "Runs when a new ticket is created.",
                category="Levels & tickets", provides=("member", "channel"), dispatched=True),
    TriggerSpec("ticket_closed", "A ticket is closed",
                "Runs when a ticket is closed or archived.",
                category="Levels & tickets", provides=("member", "channel"), dispatched=True),
    TriggerSpec("newcomer_intro_posted", "A newcomer posts their intro",
                "Runs when someone completes their introduction.",
                category="Levels & tickets", provides=("member",), dispatched=True),
    TriggerSpec("newcomer_graduated", "A newcomer becomes a member",
                "Runs when someone graduates out of the newcomer role.",
                category="Levels & tickets", provides=("member",), dispatched=True),

    TriggerSpec("schedule", "At a set time",
                "Runs on a repeating schedule instead of reacting to anything.",
                category="Time",
                fields=[
                    Field("frequency", "How often", FieldType.CHOICE,
                          choices=[("daily", "Every day"),
                                   ("weekly", "Once a week"),
                                   ("interval", "Every few hours")],
                          default="daily"),
                    Field("time_utc", "What time (UTC)", FieldType.TEXT,
                          "Use 24-hour time, like 18:30.", placeholder="18:30",
                          visible_when=lambda c: c.get("frequency") in (None, "daily", "weekly")),
                    Field("weekday", "Which day", FieldType.CHOICE,
                          choices=[(str(i), d) for i, d in enumerate(
                              ["Monday", "Tuesday", "Wednesday", "Thursday",
                               "Friday", "Saturday", "Sunday"])],
                          visible_when=lambda c: c.get("frequency") == "weekly"),
                    Field("interval_hours", "How many hours apart",
                          FieldType.NUMBER, minimum=1, maximum=168,
                          visible_when=lambda c: c.get("frequency") == "interval"),
                ]),
    TriggerSpec("manual", "Only when I run it",
                "Never runs on its own. Use the Test button, or have another "
                "automation call it.",
                category="Time"),
]}


# ------------------------------------------------------------- only if…
# Labels complete the sentence "Only run this if…".

CONDITIONS: Dict[str, ConditionSpec] = {c.key: c for c in [
    # --- the message
    ConditionSpec("content_contains", "The message contains certain words",
                  "Looks anywhere in the message.", "The message",
                  fields=[
                      Field("text", "Words or phrases to look for", FieldType.TEXT,
                            "Separate several with commas. Any one of them counts.",
                            placeholder="hello, hi, hey", required=True),
                      Field("case_sensitive", "Capital letters must match too",
                            FieldType.BOOL),
                  ],
                  run=condition_impl.content_contains, requires=("message",)),
    ConditionSpec("content_starts_with", "The message starts with something",
                  "Only looks at the beginning of the message.", "The message",
                  fields=[
                      Field("text", "Words or phrases", FieldType.TEXT,
                            "Separate several with commas. Any one of them counts.",
                            placeholder="!, ?, hey", required=True),
                      Field("case_sensitive", "Capital letters must match too",
                            FieldType.BOOL),
                  ],
                  run=condition_impl.content_starts_with, requires=("message",)),
    ConditionSpec("content_ends_with", "The message ends with something",
                  "Only looks at the end of the message.", "The message",
                  fields=[
                      Field("text", "Words or phrases", FieldType.TEXT,
                            "Separate several with commas. Any one of them counts.",
                            placeholder="?, please", required=True),
                      Field("case_sensitive", "Capital letters must match too",
                            FieldType.BOOL),
                  ],
                  run=condition_impl.content_ends_with, requires=("message",)),
    ConditionSpec("content_exactly", "The message is exactly something",
                  "The whole message and nothing else, so 'gg wp' will not match 'gg'.",
                  "The message",
                  fields=[
                      Field("text", "The exact wording", FieldType.TEXT,
                            "Separate several with commas. Any one of them counts.",
                            placeholder="gg, gg wp", required=True),
                      Field("case_sensitive", "Capital letters must match too",
                            FieldType.BOOL),
                  ],
                  run=condition_impl.content_exactly, requires=("message",)),
    ConditionSpec("is_reply", "The message is a reply",
                  "Set 'This requirement should — Not match' for messages that "
                  "aren't replies. Forwarded messages don't count as replies.",
                  "The message",
                  run=condition_impl.is_reply, requires=("message",)),
    ConditionSpec("has_image", "The message has a picture or video",
                  "Photos, screenshots and clips. Other files don't count.",
                  "The message",
                  run=condition_impl.has_image, requires=("message",)),
    ConditionSpec("has_attachment", "The message has a file attached",
                  "Any uploaded file at all, including pictures.", "The message",
                  run=condition_impl.has_attachment, requires=("message",)),
    ConditionSpec("has_link", "The message has a link",
                  "Any web address.", "The message",
                  run=condition_impl.has_link, requires=("message",)),
    ConditionSpec("has_invite", "The message has a Discord invite",
                  "An invite link to another server.", "The message",
                  run=condition_impl.has_invite, requires=("message",)),
    ConditionSpec("has_emoji", "The message has an emoji",
                  "Either standard or custom server emoji.", "The message",
                  run=condition_impl.has_emoji, requires=("message",)),
    ConditionSpec("mention_count", "The message pings people",
                  "Counts both people and roles that were pinged.", "The message",
                  fields=[Field("min", "At least this many pings",
                                FieldType.NUMBER, minimum=1, default=1)],
                  run=condition_impl.mention_count, requires=("message",)),
    ConditionSpec("caps_ratio", "The message is mostly CAPITALS",
                  "Catches shouting.", "The message",
                  fields=[
                      Field("percent", "Capitals must be at least this %",
                            FieldType.NUMBER, "Out of all the letters.",
                            minimum=1, maximum=100, default=70),
                      Field("min_length", "Ignore messages shorter than",
                            FieldType.NUMBER,
                            "Number of letters. Stops short words like 'OK' counting.",
                            minimum=1, maximum=200, default=8),
                  ],
                  run=condition_impl.caps_ratio, requires=("message",)),
    ConditionSpec("content_length", "The message is a certain length",
                  "Counts characters.", "The message",
                  fields=[Field("min", "At least this many characters",
                                FieldType.NUMBER, minimum=0),
                          Field("max", "At most this many characters",
                                FieldType.NUMBER, minimum=0)],
                  run=condition_impl.content_length, requires=("message",)),
    ConditionSpec("content_regex", "The message matches a search pattern",
                  "For advanced use. Lets you match wildcards and reuse the "
                  "matched text later as {match.1}.",
                  "The message",
                  fields=[Field("pattern", "Pattern", FieldType.TEXT,
                                "A regular expression.",
                                placeholder=r"\b(gg|good game)\b", required=True)],
                  run=condition_impl.content_regex, requires=("message",)),

    # --- who did it
    ConditionSpec("role_has", "They have (or don't have) a role",
                  "Checks the roles of the person who set this off.", "Who did it",
                  fields=[
                      Field("roles", "Which roles", FieldType.ROLE, multi=True,
                            required=True),
                      Field("match", "They must have", FieldType.CHOICE,
                            choices=[("any", "At least one of these roles"),
                                     ("all", "Every one of these roles"),
                                     ("none", "None of these roles")],
                            default="any"),
                  ],
                  run=condition_impl.role_has),
    ConditionSpec("user_is", "It's a specific person",
                  "Only runs for the people you pick.", "Who did it",
                  fields=[Field("users", "Which people", FieldType.USER, multi=True,
                                required=True)],
                  run=condition_impl.user_is),
    ConditionSpec("is_bot", "They're a bot",
                  "Whether a bot set this off rather than a person.", "Who did it",
                  run=condition_impl.is_bot),
    ConditionSpec("is_boosting", "They're boosting the server",
                  "Only runs for people boosting the server.", "Who did it",
                  run=condition_impl.is_boosting),
    ConditionSpec("has_permission", "They have a permission",
                  "Useful for letting staff skip a rule.", "Who did it",
                  fields=[Field("permission", "Which permission", FieldType.CHOICE,
                                choices=[
                                    ("administrator", "Administrator"),
                                    ("manage_guild", "Manage Server"),
                                    ("manage_messages", "Manage Messages"),
                                    ("manage_roles", "Manage Roles"),
                                    ("manage_channels", "Manage Channels"),
                                    ("moderate_members", "Time Out Members"),
                                    ("kick_members", "Kick Members"),
                                    ("ban_members", "Ban Members"),
                                    ("mention_everyone", "Mention Everyone"),
                                ], required=True)],
                  run=condition_impl.has_permission),
    ConditionSpec("account_age", "Their Discord account is a certain age",
                  "How long ago they made their Discord account.", "Who did it",
                  fields=[Field("min_days", "At least this many days old",
                                FieldType.NUMBER, minimum=0),
                          Field("max_days", "At most this many days old",
                                FieldType.NUMBER, minimum=0)],
                  run=condition_impl.account_age),
    ConditionSpec("member_age", "They've been here a certain time",
                  "How long ago they joined this server.", "Who did it",
                  fields=[Field("min_days", "Here at least this many days",
                                FieldType.NUMBER, minimum=0),
                          Field("max_days", "Here at most this many days",
                                FieldType.NUMBER, minimum=0)],
                  run=condition_impl.member_age),
    ConditionSpec("level_at_least", "They're at least a certain level",
                  "Uses their level from the economy system.", "Who did it",
                  fields=[Field("level", "Level", FieldType.NUMBER, minimum=0)],
                  run=condition_impl.level_at_least),

    # --- where it happened
    ConditionSpec("channel_is", "It's in certain channels",
                  "Picking a category covers every channel inside it. Threads "
                  "and forum posts are left out unless you switch them on below.",
                  "Where it happened",
                  fields=[Field("channels", "Which channels", FieldType.CHANNEL,
                                multi=True, required=True),
                          Field("include_threads",
                                "Also match threads and forum posts inside them",
                                FieldType.BOOL, default=False,
                                help="Off by default, so a rule pointed at a "
                                     "channel won't fire in every thread under "
                                     "it. Turn on to include them.")],
                  run=condition_impl.channel_is),
    ConditionSpec("in_thread", "It's in a thread",
                  "Set 'This requirement should — Not match' for things that "
                  "happen in the main channel instead.",
                  "Where it happened",
                  run=condition_impl.in_thread),

    # --- other
    ConditionSpec("counter_compare", "A tally has reached a number",
                  "Tallies count things over time, like warnings.", "Other",
                  fields=[
                      Field("key", "Tally name", FieldType.TEXT,
                            "Must match the name used in the 'Add to a tally' action.",
                            placeholder="strikes", required=True),
                      Field("scope", "Counted separately for", FieldType.CHOICE,
                            choices=[("user", "Each person"),
                                     ("channel", "Each channel"),
                                     ("guild", "The whole server")],
                            default="user"),
                      Field("op", "The tally must be", FieldType.CHOICE,
                            choices=[("gte", "This number or higher"),
                                     ("lte", "This number or lower"),
                                     ("eq", "Exactly this number"),
                                     ("gt", "Higher than this"),
                                     ("lt", "Lower than this")],
                            default="gte"),
                      Field("value", "Number", FieldType.NUMBER, default=1),
                  ],
                  run=condition_impl.counter_compare),
    ConditionSpec("variable_compare", "A saved note says something",
                  "Checks a value stored earlier by the 'Save a note' action.",
                  "Other",
                  fields=[
                      Field("key", "Note name", FieldType.TEXT, required=True),
                      Field("op", "The note must", FieldType.CHOICE,
                            choices=[("eq", "Be exactly this"),
                                     ("neq", "Be anything but this"),
                                     ("contains", "Contain this"),
                                     ("set", "Have anything saved in it")],
                            default="eq"),
                      Field("value", "Value", FieldType.TEXT,
                            visible_when=lambda c: c.get("op") != "set"),
                  ],
                  run=condition_impl.variable_compare),
    ConditionSpec("time_window", "It's between two times of day",
                  "Times are in UTC, and can run past midnight.", "Other",
                  fields=[Field("start", "From", FieldType.TEXT, placeholder="09:00"),
                          Field("end", "Until", FieldType.TEXT, placeholder="17:00")],
                  run=condition_impl.time_window),
    ConditionSpec("day_of_week", "It's a certain day of the week",
                  "Days are counted in UTC.", "Other",
                  fields=[Field("days", "Which days", FieldType.CHOICE, multi=True,
                                required=True,
                                choices=[(str(i), d) for i, d in enumerate(
                                    ["Monday", "Tuesday", "Wednesday", "Thursday",
                                     "Friday", "Saturday", "Sunday"])])],
                  run=condition_impl.day_of_week),
    ConditionSpec("chance", "Only some of the time",
                  "Rolls a dice, so this only runs now and then.", "Other",
                  fields=[Field("percent", "Run this % of the time",
                                FieldType.NUMBER, minimum=1, maximum=100,
                                default=50)],
                  run=condition_impl.chance),
]}


# --------------------------------------------------------- then do this…
# Labels complete the sentence "Then…".

ACTIONS: Dict[str, ActionSpec] = {a.key: a for a in [
    # --- messages
    ActionSpec("send_message", "Send a message",
               "Post in a channel, reply to them, or send them a DM. Can be a "
               "plain message or an embed with pictures.",
               "Messages",
               fields=[
                   Field("destination", "Where should it go", FieldType.CHOICE,
                         choices=[("channel", "Into a channel"),
                                  ("reply", "As a reply to their message"),
                                  ("dm", "As a private DM to them")],
                         default="channel"),
                   Field("channel", "Which channel", FieldType.CHANNEL,
                         "Leave blank to post wherever the trigger happened.",
                         needs_context="channel",
                         visible_when=lambda c: c.get("destination", "channel") == "channel"),
                   Field("content", "What should it say", FieldType.MULTILINE,
                         "You can use {user.mention}, {guild.name} and more. "
                         "With an embed on, this becomes the main text inside it."),
                   Field("ping", "Ping them in the reply", FieldType.BOOL,
                         visible_when=lambda c: c.get("destination") == "reply"),
               ] + embed_fields() + [
                   Field("add_button", "Add a button people can click",
                         FieldType.BOOL,
                         "Puts a button under the message. Anyone who clicks it "
                         "gets a private reply only they can see."),
                   Field("button_label", "Button text", FieldType.TEXT,
                         "Leave blank to show just the emoji. With no emoji "
                         "either, the button reads “Click me”.",
                         placeholder="Read the rules",
                         visible_when=lambda c: bool(c.get("add_button"))),
                   Field("button_colour", "Button colour", FieldType.CHOICE,
                         choices=[("blurple", "Blurple"), ("green", "Green"),
                                  ("grey", "Grey"), ("red", "Red")],
                         default="blurple",
                         visible_when=lambda c: bool(c.get("add_button"))),
                   Field("button_emoji", "Button emoji (optional)",
                         FieldType.EMOJI,
                         "One emoji, standard or from any server the bot is in.",
                         visible_when=lambda c: bool(c.get("add_button"))),
                   Field("reply_content", "What the button shows", FieldType.MULTILINE,
                         "The private message the clicker sees. You can use "
                         "{user.mention}, {guild.name} and more.",
                         visible_when=lambda c: bool(c.get("add_button"))),
               ] + embed_fields(prefix="reply_",
                                 gate=lambda c: bool(c.get("add_button"))),
               run=action_impl.send_message, perms=("send_messages",)),
    ActionSpec("delete_message", "Delete their message",
               "Removes the message that set this off.", "Messages",
               run=action_impl.delete_message, perms=("manage_messages",),
               requires=("message",), destructive=True),
    ActionSpec("add_reaction", "React to a message",
               "Adds as many reactions as you like, in the order you list them.",
               "Messages",
               fields=[
                   Field("emoji", "Which emoji", FieldType.EMOJI,
                         "Put several in, separated by spaces. They're added "
                         "left to right."),
                   _which_message(),
               ],
               run=action_impl.add_reaction, perms=("add_reactions",)),
    ActionSpec("remove_reaction", "Take back my reaction",
               "Removes a reaction the bot added earlier.", "Messages",
               fields=[Field("emoji", "Which emoji", FieldType.EMOJI)],
               run=action_impl.remove_reaction, requires=("message",)),
    ActionSpec("clear_reactions", "Clear everyone's reactions",
               "Wipes all reactions off the message.", "Messages",
               run=action_impl.clear_reactions, perms=("manage_messages",),
               requires=("message",), destructive=True),
    ActionSpec("create_thread", "Start a thread on a message",
               "Opens a thread for people to talk in.", "Messages",
               fields=[
                   Field("name", "Thread name", FieldType.TEXT,
                         placeholder="{user} — discussion"),
                   Field("message", "First message in the thread",
                         FieldType.MULTILINE, "Leave blank to post nothing."),
                   Field("archive_minutes", "Close it after no activity for",
                         FieldType.CHOICE,
                         choices=[(60, "1 hour"), (1440, "1 day"),
                                  (4320, "3 days"), (10080, "1 week")],
                         default=1440),
                   _which_message(),
               ],
               run=action_impl.create_thread, perms=("create_public_threads",)),
    ActionSpec("pin_message", "Pin a message",
               "Adds it to the channel's pins.", "Messages",
               fields=[_which_message()],
               run=action_impl.pin_message, perms=("manage_messages",)),
    ActionSpec("unpin_message", "Unpin their message",
               "Takes it back out of the pins.", "Messages",
               run=action_impl.unpin_message, perms=("manage_messages",),
               requires=("message",)),
    ActionSpec("publish_message", "Publish their message",
               "Sends it out to servers following this announcement channel.",
               "Messages",
               run=action_impl.publish_message, perms=("manage_messages",),
               requires=("message",)),

    # --- roles and names
    ActionSpec("add_role", "Give them a role",
               "Optionally take it back again after a while.", "Roles & names",
               fields=[
                   Field("role", "Which role", FieldType.ROLE, required=True),
                   Field("duration", "Take it back after", FieldType.DURATION,
                         "Leave blank to keep it forever. This still works if "
                         "the bot restarts."),
               ],
               run=action_impl.add_role, perms=("manage_roles",)),
    ActionSpec("remove_role", "Take a role away",
               "Removes the role if they have it.", "Roles & names",
               fields=[Field("role", "Which role", FieldType.ROLE, required=True)],
               run=action_impl.remove_role, perms=("manage_roles",)),
    ActionSpec("toggle_role", "Give or take a role",
               "Gives it if they don't have it, takes it if they do. Good for "
               "self-serve roles.", "Roles & names",
               fields=[Field("role", "Which role", FieldType.ROLE, required=True)],
               run=action_impl.toggle_role, perms=("manage_roles",)),
    ActionSpec("set_nickname", "Change their nickname",
               "Leave it blank to reset their nickname.", "Roles & names",
               fields=[Field("nickname", "New nickname", FieldType.TEXT,
                             placeholder="{user.name} 🌟")],
               run=action_impl.set_nickname, perms=("manage_nicknames",)),

    # --- moderation
    ActionSpec("timeout_member", "Time them out",
               "Stops them talking for a while. Kicking and banning are handled "
               "by the security tools, not here.",
               "Moderation",
               fields=[
                   Field("duration", "For how long", FieldType.DURATION,
                         "Anything from 1 minute to 28 days.", default=600),
                   Field("reason", "Reason", FieldType.TEXT,
                         "Shows up in the audit log."),
               ],
               run=action_impl.timeout_member, perms=("moderate_members",),
               destructive=True),
    ActionSpec("remove_timeout", "End their timeout early",
               "Lets them talk again straight away.", "Moderation",
               run=action_impl.remove_timeout, perms=("moderate_members",)),
    ActionSpec("set_slowmode", "Change a channel's slowmode",
               "Set it to 0 to turn slowmode off.", "Moderation",
               fields=[
                   Field("channel", "Which channel", FieldType.CHANNEL,
                         "Leave blank to use the channel this happened in.",
                         needs_context="channel"),
                   Field("seconds", "Seconds between messages",
                         FieldType.NUMBER, minimum=0, maximum=21600),
               ],
               run=action_impl.set_slowmode, perms=("manage_channels",)),
    ActionSpec("voice_disconnect", "Disconnect them from voice",
               "Kicks them out of the voice channel they're in.", "Moderation",
               run=action_impl.voice_disconnect, perms=("move_members",),
               destructive=True),
    ActionSpec("voice_move", "Move them to a voice channel",
               "Only works if they're already in voice.", "Moderation",
               fields=[Field("channel", "Which voice channel", FieldType.CHANNEL,
                             required=True)],
               run=action_impl.voice_move, perms=("move_members",)),

    # --- remembering things
    ActionSpec("counter_change", "Add to a tally",
               "Tallies count things over time — warnings, wins, anything. "
               "Check one later with 'A tally has reached a number'.",
               "Remembering things",
               fields=[
                   Field("key", "Tally name", FieldType.TEXT,
                         "Make one up. Use the same name to check it later.",
                         placeholder="strikes"),
                   Field("scope", "Counted separately for", FieldType.CHOICE,
                         choices=[("user", "Each person"),
                                  ("channel", "Each channel"),
                                  ("guild", "The whole server")],
                         default="user"),
                   Field("amount", "Add this much", FieldType.NUMBER,
                         "Use a negative number to subtract.", default=1),
                   Field("window", "Forget it again after", FieldType.DURATION,
                         "Leave blank to remember forever. Set 1d for "
                         "'3 strikes in a day'."),
               ],
               run=action_impl.counter_change),
    ActionSpec("counter_reset", "Reset a tally to zero",
               "Wipes the count and starts again.", "Remembering things",
               fields=[
                   Field("key", "Tally name", FieldType.TEXT),
                   Field("scope", "Counted separately for", FieldType.CHOICE,
                         choices=[("user", "Each person"),
                                  ("channel", "Each channel"),
                                  ("guild", "The whole server")],
                         default="user"),
               ],
               run=action_impl.counter_reset),
    ActionSpec("set_variable", "Save a note for later steps",
               "Stores a value you can use further down as {var.name}. It's "
               "forgotten once this automation finishes.",
               "Remembering things",
               fields=[Field("key", "Note name", FieldType.TEXT),
                       Field("value", "What to save", FieldType.TEXT)],
               run=action_impl.set_variable),
    ActionSpec("award_points", "Give them Points",
               "Adds to their balance in the economy system.",
               "Remembering things",
               fields=[Field("amount", "How many Points", FieldType.NUMBER,
                             default=10),
                       Field("reason", "Reason", FieldType.TEXT)],
               run=action_impl.award_points),

    # --- controlling the automation
    ActionSpec("log_line", "Write to a log channel",
               "Posts a note so staff can see this ran.", "Controls",
               fields=[Field("channel", "Which channel", FieldType.CHANNEL),
                       Field("content", "What to write", FieldType.MULTILINE)]
               + embed_fields(),
               run=action_impl.log_line, perms=("send_messages",)),
    ActionSpec("wait", "Wait a while",
               "Pauses before the next step. This still works if the bot "
               "restarts partway through.", "Controls",
               fields=[Field("duration", "Wait for", FieldType.DURATION,
                             default=60)],
               run=action_impl.wait),
    ActionSpec("stop", "Stop here",
               "Skips everything after this point.", "Controls",
               run=action_impl.stop),
    ActionSpec("run_automation", "Run another automation",
               "Hands over to one of your other automations.", "Controls",
               fields=[Field("automation", "Which one", FieldType.CHOICE)],
               run=action_impl.run_automation),
]}


# ------------------------------------------------------------------ lookups

def categories(specs: dict) -> Dict[str, list]:
    """Group a catalogue by category, keeping declaration order."""
    grouped: Dict[str, list] = {}
    for spec in specs.values():
        grouped.setdefault(spec.category, []).append(spec)
    return grouped


def required_permissions(steps) -> set:
    """Every permission the steps in this automation collectively need."""
    needed = set()
    for step in steps:
        if step.is_branch:
            needed |= required_permissions(step.then)
            needed |= required_permissions(step.otherwise)
            continue
        spec = ACTIONS.get(step.type)
        if spec:
            needed |= set(spec.perms)
    return needed


# What each permission is actually called in Discord's own settings, so the
# warning tells you what to go and tick rather than naming an API flag.
PERMISSION_NAMES = {
    "send_messages": "Send Messages",
    "manage_messages": "Manage Messages",
    "manage_roles": "Manage Roles",
    "manage_channels": "Manage Channels",
    "manage_nicknames": "Manage Nicknames",
    "moderate_members": "Time Out Members",
    "move_members": "Move Members",
    "add_reactions": "Add Reactions",
    "create_public_threads": "Create Public Threads",
}


def permission_name(key: str) -> str:
    return PERMISSION_NAMES.get(key, key.replace("_", " ").title())


def missing_permissions(guild, steps) -> list:
    """Which of those the bot does not hold, for the warning banner."""
    me = getattr(guild, "me", None)
    if me is None:
        return []
    held = me.guild_permissions
    if held.administrator:
        return []
    return sorted(p for p in required_permissions(steps) if not getattr(held, p, False))
