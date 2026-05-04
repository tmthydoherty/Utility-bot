from typing import Dict, Any

# Default cooldown in minutes
DEFAULT_COOLDOWN_MINUTES = 5


def _ensure_topic_defaults(t: Dict[str, Any]) -> Dict[str, Any]:
    t.setdefault("name", "new-topic")
    t.setdefault("label", "New Topic")
    t.setdefault("emoji", None)
    t.setdefault("type", "ticket")  # ticket, application, survey - semantic label only
    t.setdefault("mode", "thread")
    t.setdefault("parent_id", None)
    t.setdefault("staff_role_ids", [])
    t.setdefault("log_channel_id", None)
    t.setdefault("welcome_message", "Welcome {user}! A staff member will be with you shortly.\nTopic: **{topic}**")
    t.setdefault("questions", [])
    t.setdefault("approval_mode", False)
    t.setdefault("discussion_mode", False)
    t.setdefault("application_channel_mode", "dm")  # "dm" or "channel"
    t.setdefault("button_color", "secondary")

    # Pre-modal question 1
    t.setdefault("pre_modal_enabled", False)
    t.setdefault("pre_modal_question", "Do you have your profile link ready?")
    t.setdefault("pre_modal_redirect_url", None)
    t.setdefault("pre_modal_redirect_channel_id", None)
    t.setdefault("pre_modal_yes_label", "Yes, I have it")
    t.setdefault("pre_modal_no_label", "No, I need to get it")
    t.setdefault("pre_modal_no_message", "Please get what you need ready, then click the button below to continue.")
    t.setdefault("pre_modal_ready_button_enabled", True)
    t.setdefault("pre_modal_ready_button_label", "I'm ready now")

    # Pre-modal required answer
    t.setdefault("pre_modal_answer_enabled", False)
    t.setdefault("pre_modal_answer_question", "Please provide your information:")

    # Pre-modal question 2
    t.setdefault("pre_modal_2_enabled", False)
    t.setdefault("pre_modal_2_question", "Do you have your second requirement ready?")
    t.setdefault("pre_modal_2_redirect_url", None)
    t.setdefault("pre_modal_2_redirect_channel_id", None)
    t.setdefault("pre_modal_2_yes_label", "Yes")
    t.setdefault("pre_modal_2_no_label", "No")
    t.setdefault("pre_modal_2_no_message", "Please get what you need ready, then click the button below to continue.")
    t.setdefault("pre_modal_2_ready_button_enabled", True)
    t.setdefault("pre_modal_2_ready_button_label", "I'm ready now")

    # Unified cooldown (migrated from survey_cooldown_minutes)
    if "survey_cooldown_minutes" in t and "cooldown_minutes" not in t:
        t["cooldown_minutes"] = t.pop("survey_cooldown_minutes")
    elif "survey_cooldown_minutes" in t:
        t.pop("survey_cooldown_minutes", None)
    t.setdefault("cooldown_minutes", DEFAULT_COOLDOWN_MINUTES)

    # Staff notification
    t.setdefault("ping_staff_on_create", False)

    # Close settings
    t.setdefault("delete_on_close", True)
    t.setdefault("member_can_close", True)

    # Claim system
    t.setdefault("claim_enabled", False)
    t.setdefault("claim_alerts_channel_id", None)
    t.setdefault("claim_role_id", None)

    # --- QoL Batch 1 ---
    # Ticket numbering
    t.setdefault("use_numbering", False)
    t.setdefault("ticket_counter", 0)

    # Custom channel/thread name format
    # Placeholders: {user}, {topic}, {number}
    t.setdefault("channel_name_format", None)  # None = legacy behavior

    # Custom close message
    t.setdefault("close_message", "Your ticket `{channel}` in **{server}** has been closed by {closer}.")

    # Blacklist
    t.setdefault("blacklisted_user_ids", [])

    # Embed color (hex string)
    t.setdefault("embed_color", None)  # None = use default discord colors

    return t


def _ensure_panel_defaults(p: Dict[str, Any]) -> Dict[str, Any]:
    p.setdefault("name", "new-panel")
    p.setdefault("channel_id", None)
    p.setdefault("title", "Support Panel")
    p.setdefault("description", "Please select an option below.")
    p.setdefault("display_mode", "buttons")
    p.setdefault("message_id", None)
    p.setdefault("topic_names", [])
    p.setdefault("image_url", None)
    p.setdefault("image_type", "banner")
    # Mixed mode: per-panel topic ordering and button/dropdown assignment
    p.setdefault("topic_order", [])          # ordered list of topic names
    p.setdefault("topic_display_map", {})    # topic_name -> "button" or "dropdown"
    p.setdefault("categories", {})           # cat_slug -> {label, emoji, display_mode, topic_names, button_color}
    return p


USERNAME_MAX_LEN = 9


def format_channel_name(topic: Dict[str, Any], member_name: str) -> str:
    """Build a channel/thread name from the topic's format string.

    Supported placeholders: {user}, {topic}, {number}
    If no custom format is set, falls back to legacy behaviour.
    """
    fmt = topic.get("channel_name_format")
    topic_name = topic.get("name", "ticket")
    truncated_user = member_name[:USERNAME_MAX_LEN]

    if fmt:
        counter = topic.get("ticket_counter", 0)
        name = fmt.format(
            user=truncated_user,
            topic=topic_name,
            number=f"{counter:04d}",
        )
    elif topic.get("use_numbering", False):
        counter = topic.get("ticket_counter", 0)
        name = f"{topic_name}-{counter:04d}"
    else:
        name = f"{topic_name}-{truncated_user}"

    return name.replace(" ", "-").lower()[:100]


def get_embed_color(topic: Dict[str, Any], fallback: int = 0x5865F2) -> int:
    """Get the embed color for a topic, falling back to default."""
    hex_color = topic.get("embed_color")
    if hex_color:
        try:
            return int(hex_color.lstrip("#"), 16)
        except (ValueError, AttributeError):
            pass
    return fallback
