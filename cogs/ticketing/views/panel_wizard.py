import discord
import logging
from typing import Dict, Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..cog import TicketSystem

from ..storage import _load_json, _save_json, TOPICS_FILE, PANELS_FILE
from ..defaults import _ensure_panel_defaults

logger = logging.getLogger("ticketing_cog")


class PanelWizardView(discord.ui.View):
    def __init__(self, cog: "TicketSystem", interaction: discord.Interaction,
                 panel_data: Dict[str, Any], is_new: bool, all_topics: Dict[str, Any]):
        super().__init__(timeout=600)
        self.cog = cog
        self.original_interaction = interaction
        self.panel_data = panel_data
        self.is_new = is_new
        self.all_topics = all_topics
        self.wizard_message = None
        self.subpage = None
        self.update_components()

    # --- Helpers ---

    async def _refresh(self, interaction: discord.Interaction):
        """Called from modal on_submit callbacks. Defers the interaction and updates the wizard."""
        if not interaction.response.is_done():
            await interaction.response.defer()
        self.update_components()
        try:
            if self.wizard_message:
                await self.wizard_message.edit(embed=self._embed(), view=self)
            else:
                await self.original_interaction.edit_original_response(embed=self._embed(), view=self)
        except discord.NotFound:
            pass

    async def _edit(self, interaction: discord.Interaction):
        """Update the wizard message via a component interaction (button/select)."""
        self.update_components()
        await interaction.response.edit_message(embed=self._embed(), view=self)

    # --- Component Builder ---

    def update_components(self):
        self.clear_items()
        if self.subpage:
            self._build_subpage()
            return
        self.add_item(self._btn("Edit Title/Desc", discord.ButtonStyle.secondary, self.edit_text_cb, 0))
        self.add_item(self._btn("Set Post Channel", discord.ButtonStyle.secondary, self.set_channel_cb, 0))
        self.add_item(self._btn("Set Image", discord.ButtonStyle.secondary, self.set_image_cb, 0))
        mode = self.panel_data.get('display_mode', 'buttons')
        self.add_item(self._btn(f"Display: {mode.capitalize()}", discord.ButtonStyle.primary, self.toggle_display_cb, 1))
        self.add_item(self._btn("Manage Topics", discord.ButtonStyle.blurple, self.manage_topics_cb, 1))
        if mode == 'mixed':
            self.add_item(self._btn("Topic Layout", discord.ButtonStyle.secondary, self.topic_layout_cb, 2))
        has_topics = bool(self.panel_data.get('topic_names'))
        self.add_item(self._btn("Topic Order", discord.ButtonStyle.secondary, self.topic_order_cb, 2, disabled=not has_topics))
        cat_count = len(self.panel_data.get('categories', {}))
        cat_label = f"Categories ({cat_count})" if cat_count else "Categories"
        self.add_item(self._btn(cat_label, discord.ButtonStyle.secondary, self.manage_categories_cb, 3, disabled=not has_topics))
        self.add_item(self._btn("Finish & Save", discord.ButtonStyle.success, self.finish_cb, 4))
        self.add_item(self._btn("Cancel", discord.ButtonStyle.danger, self.cancel_cb, 4))

    def _btn(self, label, style, callback, row, disabled=False):
        btn = discord.ui.Button(label=label, style=style, row=row, disabled=disabled)
        btn.callback = callback
        return btn

    # --- Subpage Builder ---

    def _build_subpage(self):
        sp = self.subpage
        sp_type = sp["type"]

        if sp_type == "channel_picker":
            select = discord.ui.ChannelSelect(
                placeholder="Select a channel...",
                channel_types=[discord.ChannelType.text],
                row=0
            )

            async def _pick(itx: discord.Interaction):
                self.panel_data['channel_id'] = int(itx.data['values'][0])
                self.subpage = None
                await self._edit(itx)

            select.callback = _pick
            self.add_item(select)

        elif sp_type == "image_picker":
            select = discord.ui.Select(
                placeholder="Choose image type or remove image...",
                options=[
                    discord.SelectOption(label="Banner (e.g., 1200x400)", value="banner"),
                    discord.SelectOption(label="Thumbnail (e.g., 256x256)", value="thumbnail"),
                    discord.SelectOption(label="Remove Image", value="remove"),
                ],
                row=0
            )

            async def _pick(itx: discord.Interaction):
                image_type = itx.data['values'][0]
                if image_type == "remove":
                    self.panel_data['image_url'] = None
                    self.subpage = None
                    await self._edit(itx)
                else:
                    await itx.response.send_modal(ImageUrlModal(self, image_type))

            select.callback = _pick
            self.add_item(select)

        elif sp_type == "topic_manager":
            all_topics = sp.get("topics", {})
            if all_topics:
                options = [
                    discord.SelectOption(
                        label=f"{t.get('emoji') or ''} {t.get('label')}"[:100],
                        value=name[:100],
                        description=f"Type: {t.get('type', 'N/A').capitalize()}"[:100],
                        default=name in self.panel_data.get('topic_names', [])
                    ) for name, t in all_topics.items()
                ][:25]
                select = discord.ui.Select(
                    placeholder="Select topics...",
                    options=options,
                    min_values=0, max_values=len(options),
                    row=0
                )

                async def _pick(itx: discord.Interaction):
                    self.panel_data['topic_names'] = itx.data['values']
                    await self._edit(itx)

                select.callback = _pick
                self.add_item(select)

        elif sp_type == "topic_layout":
            # Let the user pick which attached topics go in the dropdown
            attached = self.panel_data.get('topic_names', [])
            display_map = self.panel_data.get('topic_display_map', {})
            all_topics = sp.get("topics", {})
            if attached:
                options = []
                for name in attached:
                    t = all_topics.get(name, {})
                    lbl = f"{t.get('emoji') or ''} {t.get('label', name)}"[:100]
                    options.append(discord.SelectOption(
                        label=lbl, value=name[:100],
                        description="Currently: Dropdown" if display_map.get(name) == 'dropdown' else "Currently: Button",
                        default=display_map.get(name) == 'dropdown'
                    ))
                options = options[:25]
                select = discord.ui.Select(
                    placeholder="Select topics for the dropdown...",
                    options=options,
                    min_values=0, max_values=len(options),
                    row=0
                )

                async def _pick_layout(itx: discord.Interaction):
                    dropdown_names = set(itx.data.get('values', []))
                    new_map = {}
                    for n in self.panel_data.get('topic_names', []):
                        new_map[n] = 'dropdown' if n in dropdown_names else 'button'
                    self.panel_data['topic_display_map'] = new_map
                    await self._edit(itx)

                select.callback = _pick_layout
                self.add_item(select)

        elif sp_type == "topic_order":
            attached = self.panel_data.get('topic_names', [])
            categories = self.panel_data.get('categories', {})
            order = self.panel_data.get('topic_order', [])
            all_topics = sp.get("topics", {})

            # Determine categorized topics (hidden from direct ordering)
            categorized = set()
            for cat_data in categories.values():
                for tn in cat_data.get('topic_names', []):
                    if tn in attached:
                        categorized.add(tn)

            # Build effective order: uncategorized topics + cat: entries
            valid_items = set(n for n in attached if n not in categorized) | {f"cat:{s}" for s in categories}
            ordered = [n for n in order if n in valid_items]
            for n in attached:
                if n not in categorized and n not in ordered:
                    ordered.append(n)
            for slug in categories:
                if f"cat:{slug}" not in ordered:
                    ordered.append(f"cat:{slug}")

            def _make_options():
                opts = []
                for i, item in enumerate(ordered):
                    if item.startswith("cat:"):
                        slug = item[4:]
                        cat = categories.get(slug, {})
                        lbl = f"{i+1}. \U0001f4c1 {cat.get('label', slug)}"[:100]
                    else:
                        t = all_topics.get(item, {})
                        lbl = f"{i+1}. {t.get('emoji') or ''} {t.get('label', item)}"[:100]
                    opts.append(discord.SelectOption(label=lbl, value=item[:100]))
                return opts[:25]

            if ordered:
                select = discord.ui.Select(
                    placeholder="Select an item to move up...",
                    options=_make_options(),
                    min_values=1, max_values=1,
                    row=0
                )

                async def _move_up(itx: discord.Interaction):
                    name = itx.data['values'][0]
                    att = self.panel_data.get('topic_names', [])
                    cats = self.panel_data.get('categories', {})
                    cur_order = self.panel_data.get('topic_order', [])
                    cat_set = set()
                    for cd in cats.values():
                        for tn in cd.get('topic_names', []):
                            if tn in att:
                                cat_set.add(tn)
                    vi = set(n for n in att if n not in cat_set) | {f"cat:{s}" for s in cats}
                    eff = [n for n in cur_order if n in vi]
                    for n in att:
                        if n not in cat_set and n not in eff:
                            eff.append(n)
                    for s in cats:
                        if f"cat:{s}" not in eff:
                            eff.append(f"cat:{s}")
                    idx = eff.index(name) if name in eff else -1
                    if idx > 0:
                        eff[idx], eff[idx - 1] = eff[idx - 1], eff[idx]
                    self.panel_data['topic_order'] = eff
                    self.subpage["topics"] = sp.get("topics", {})
                    await self._edit(itx)

                select.callback = _move_up
                self.add_item(select)

            if len(ordered) > 1:
                select2 = discord.ui.Select(
                    placeholder="Select an item to move down...",
                    options=_make_options(),
                    min_values=1, max_values=1,
                    row=1
                )

                async def _move_down(itx: discord.Interaction):
                    name = itx.data['values'][0]
                    att = self.panel_data.get('topic_names', [])
                    cats = self.panel_data.get('categories', {})
                    cur_order = self.panel_data.get('topic_order', [])
                    cat_set = set()
                    for cd in cats.values():
                        for tn in cd.get('topic_names', []):
                            if tn in att:
                                cat_set.add(tn)
                    vi = set(n for n in att if n not in cat_set) | {f"cat:{s}" for s in cats}
                    eff = [n for n in cur_order if n in vi]
                    for n in att:
                        if n not in cat_set and n not in eff:
                            eff.append(n)
                    for s in cats:
                        if f"cat:{s}" not in eff:
                            eff.append(f"cat:{s}")
                    idx = eff.index(name) if name in eff else -1
                    if 0 <= idx < len(eff) - 1:
                        eff[idx], eff[idx + 1] = eff[idx + 1], eff[idx]
                    self.panel_data['topic_order'] = eff
                    self.subpage["topics"] = sp.get("topics", {})
                    await self._edit(itx)

                select2.callback = _move_down
                self.add_item(select2)

        elif sp_type == "category_manager":
            categories = self.panel_data.get('categories', {})
            if categories:
                options = [
                    discord.SelectOption(
                        label=f"{c.get('emoji') or ''} {c.get('label', slug)}"[:100],
                        value=slug[:100]
                    ) for slug, c in categories.items()
                ][:25]
                select = discord.ui.Select(placeholder="Select a category to edit...", options=options, row=0)

                async def _edit_cat(itx: discord.Interaction):
                    all_topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
                    self.subpage = {"type": "category_edit", "cat_slug": itx.data['values'][0], "topics": all_topics}
                    await self._edit(itx)

                select.callback = _edit_cat
                self.add_item(select)

            self.add_item(self._btn("Create Category", discord.ButtonStyle.success, self._cat_create_cb, 2))
            if categories:
                self.add_item(self._btn("Delete Category", discord.ButtonStyle.danger, self._cat_delete_cb, 2))

        elif sp_type == "category_edit":
            cat_slug = sp["cat_slug"]
            cat_data = self.panel_data.get('categories', {}).get(cat_slug, {})
            attached = self.panel_data.get('topic_names', [])
            all_topics = sp.get("topics", {})

            if attached:
                options = []
                for n in attached:
                    t = all_topics.get(n, {})
                    lbl = f"{t.get('emoji') or ''} {t.get('label', n)}"[:100]
                    options.append(discord.SelectOption(
                        label=lbl, value=n[:100],
                        default=n in cat_data.get('topic_names', [])
                    ))
                options = options[:25]
                select = discord.ui.Select(
                    placeholder="Select topics for this category...",
                    options=options, min_values=0, max_values=len(options), row=0
                )

                async def _pick_cat_topics(itx: discord.Interaction):
                    self.panel_data.setdefault('categories', {})[cat_slug]['topic_names'] = itx.data.get('values', [])
                    self.subpage["topics"] = sp.get("topics", {})
                    await self._edit(itx)

                select.callback = _pick_cat_topics
                self.add_item(select)

            mode = cat_data.get('display_mode', 'buttons')
            color = cat_data.get('button_color', 'primary')

            async def _toggle_cat_display(itx: discord.Interaction):
                cur = self.panel_data['categories'][cat_slug].get('display_mode', 'buttons')
                self.panel_data['categories'][cat_slug]['display_mode'] = 'dropdown' if cur == 'buttons' else 'buttons'
                self.subpage["topics"] = sp.get("topics", {})
                await self._edit(itx)

            async def _toggle_cat_color(itx: discord.Interaction):
                cycle = ['primary', 'secondary', 'success', 'danger']
                cur = self.panel_data['categories'][cat_slug].get('button_color', 'primary')
                idx = cycle.index(cur) if cur in cycle else 0
                self.panel_data['categories'][cat_slug]['button_color'] = cycle[(idx + 1) % len(cycle)]
                self.subpage["topics"] = sp.get("topics", {})
                await self._edit(itx)

            async def _rename_cat(itx: discord.Interaction):
                await itx.response.send_modal(EditCategoryModal(self, cat_slug))

            async def _cat_topic_order(itx: discord.Interaction):
                if len(cat_data.get('topic_names', [])) < 2:
                    return await itx.response.send_message("Need at least 2 topics to reorder.", ephemeral=True, delete_after=10)
                self.subpage = {"type": "category_topic_order", "cat_slug": cat_slug, "topics": sp.get("topics", {})}
                await self._edit(itx)

            self.add_item(self._btn(f"Ephemeral: {mode.capitalize()}", discord.ButtonStyle.primary, _toggle_cat_display, 2))
            self.add_item(self._btn(f"Color: {color.capitalize()}", discord.ButtonStyle.secondary, _toggle_cat_color, 2))
            has_cat_topics = len(cat_data.get('topic_names', [])) >= 2
            self.add_item(self._btn("Topic Order", discord.ButtonStyle.secondary, _cat_topic_order, 3, disabled=not has_cat_topics))
            self.add_item(self._btn("Edit Label", discord.ButtonStyle.secondary, _rename_cat, 3))

        elif sp_type == "category_topic_order":
            cat_slug = sp["cat_slug"]
            cat_data = self.panel_data.get('categories', {}).get(cat_slug, {})
            cat_topics = cat_data.get('topic_names', [])
            all_topics = sp.get("topics", {})

            def _make_cat_topic_options():
                opts = []
                for i, name in enumerate(cat_topics):
                    t = all_topics.get(name, {})
                    lbl = f"{i+1}. {t.get('emoji') or ''} {t.get('label', name)}"[:100]
                    opts.append(discord.SelectOption(label=lbl, value=name[:100]))
                return opts[:25]

            if cat_topics:
                select = discord.ui.Select(
                    placeholder="Select a topic to move up...",
                    options=_make_cat_topic_options(),
                    min_values=1, max_values=1,
                    row=0
                )

                async def _move_up(itx: discord.Interaction):
                    name = itx.data['values'][0]
                    names = self.panel_data['categories'][cat_slug].get('topic_names', [])
                    idx = names.index(name) if name in names else -1
                    if idx > 0:
                        names[idx], names[idx - 1] = names[idx - 1], names[idx]
                    self.subpage["topics"] = sp.get("topics", {})
                    await self._edit(itx)

                select.callback = _move_up
                self.add_item(select)

            if len(cat_topics) > 1:
                select2 = discord.ui.Select(
                    placeholder="Select a topic to move down...",
                    options=_make_cat_topic_options(),
                    min_values=1, max_values=1,
                    row=1
                )

                async def _move_down(itx: discord.Interaction):
                    name = itx.data['values'][0]
                    names = self.panel_data['categories'][cat_slug].get('topic_names', [])
                    idx = names.index(name) if name in names else -1
                    if 0 <= idx < len(names) - 1:
                        names[idx], names[idx + 1] = names[idx + 1], names[idx]
                    self.subpage["topics"] = sp.get("topics", {})
                    await self._edit(itx)

                select2.callback = _move_down
                self.add_item(select2)

        elif sp_type == "category_delete":
            categories = self.panel_data.get('categories', {})
            if categories:
                options = [
                    discord.SelectOption(
                        label=f"{c.get('emoji') or ''} {c.get('label', slug)}"[:100],
                        value=slug[:100]
                    ) for slug, c in categories.items()
                ][:25]
                select = discord.ui.Select(placeholder="Select a category to delete...", options=options, row=0)

                async def _del_cat(itx: discord.Interaction):
                    slug = itx.data['values'][0]
                    self.panel_data.get('categories', {}).pop(slug, None)
                    order = self.panel_data.get('topic_order', [])
                    self.panel_data['topic_order'] = [x for x in order if x != f"cat:{slug}"]
                    all_topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
                    self.subpage = {"type": "category_manager", "topics": all_topics}
                    await self._edit(itx)

                select.callback = _del_cat
                self.add_item(select)

        self.add_item(self._btn("\u25c0 Back", discord.ButtonStyle.secondary, self._subpage_back_cb, 4))

    async def _subpage_back_cb(self, interaction: discord.Interaction):
        sp = self.subpage
        # Navigate back to category_manager from category sub-subpages
        if sp and sp.get("type") == "category_topic_order":
            all_topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
            self.subpage = {"type": "category_edit", "cat_slug": sp["cat_slug"], "topics": all_topics}
        elif sp and sp.get("type") in ("category_edit", "category_delete"):
            all_topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
            self.subpage = {"type": "category_manager", "topics": all_topics}
        else:
            self.subpage = None
        await self._edit(interaction)

    # --- Embed ---

    def _embed(self) -> discord.Embed:
        if self.subpage:
            return self._subpage_embed()
        return self._wizard_embed()

    def _subpage_embed(self) -> discord.Embed:
        sp = self.subpage
        sp_type = sp["type"]
        panel_name = self.panel_data.get('name', '')
        base = f"`{panel_name}` \u2014 "

        if sp_type == "channel_picker":
            current = self.panel_data.get('channel_id')
            embed = discord.Embed(title=base + "Post Channel", color=discord.Color.purple())
            embed.description = f"Current: <#{current}>" if current else "Current: *Not set*"
            return embed
        elif sp_type == "image_picker":
            image_url = self.panel_data.get('image_url')
            image_type = self.panel_data.get('image_type', 'banner')
            embed = discord.Embed(title=base + "Image", color=discord.Color.purple())
            if image_url:
                embed.description = f"Current: **{image_type.capitalize()}**\n{image_url}"
            else:
                embed.description = "Current: *Not set*"
            return embed
        elif sp_type == "topic_manager":
            attached = self.panel_data.get('topic_names', [])
            topic_text = "\n".join([f"\u2022 `{name}`" for name in attached]) or "*None*"
            embed = discord.Embed(title=base + "Attached Topics", color=discord.Color.purple())
            embed.description = f"**Current ({len(attached)}):**\n{topic_text}"
            return embed

        elif sp_type == "topic_layout":
            display_map = self.panel_data.get('topic_display_map', {})
            attached = self.panel_data.get('topic_names', [])
            buttons = [n for n in attached if display_map.get(n, 'button') == 'button']
            dropdowns = [n for n in attached if display_map.get(n) == 'dropdown']
            embed = discord.Embed(title=base + "Topic Layout", color=discord.Color.purple())
            btn_text = "\n".join([f"\u2022 `{n}`" for n in buttons]) or "*None*"
            dd_text = "\n".join([f"\u2022 `{n}`" for n in dropdowns]) or "*None*"
            embed.description = (
                "Select which topics appear in the **dropdown** (unselected = button).\n\n"
                f"**Buttons:**\n{btn_text}\n\n**Dropdown (\"Anything else\"):**\n{dd_text}"
            )
            return embed

        elif sp_type == "topic_order":
            attached = self.panel_data.get('topic_names', [])
            categories = self.panel_data.get('categories', {})
            order = self.panel_data.get('topic_order', [])
            all_topics = sp.get("topics", {})

            categorized = set()
            for cat_data in categories.values():
                for tn in cat_data.get('topic_names', []):
                    if tn in attached:
                        categorized.add(tn)

            valid_items = set(n for n in attached if n not in categorized) | {f"cat:{s}" for s in categories}
            ordered = [n for n in order if n in valid_items]
            for n in attached:
                if n not in categorized and n not in ordered:
                    ordered.append(n)
            for slug in categories:
                if f"cat:{slug}" not in ordered:
                    ordered.append(f"cat:{slug}")

            lines = []
            for i, item in enumerate(ordered):
                if item.startswith("cat:"):
                    slug = item[4:]
                    cat = categories.get(slug, {})
                    lines.append(f"`{i+1}.` \U0001f4c1 {cat.get('label', slug)}")
                else:
                    t = all_topics.get(item, {})
                    emoji = t.get('emoji') or ''
                    label = t.get('label', item)
                    lines.append(f"`{i+1}.` {emoji} {label}")
            embed = discord.Embed(title=base + "Topic Order", color=discord.Color.purple())
            embed.description = (
                "Use the selects to reorder items.\n"
                "**Move Up** shifts the selected item up one position.\n"
                "**Move Down** shifts it down.\n\n"
                + ("\n".join(lines) or "*No items*")
            )
            return embed

        elif sp_type == "category_manager":
            categories = self.panel_data.get('categories', {})
            all_topics = sp.get("topics", {})
            embed = discord.Embed(title=base + "Categories", color=discord.Color.purple())
            if not categories:
                embed.description = "*No categories created yet.* Use **Create Category** to get started."
            else:
                lines = []
                for slug, c in categories.items():
                    emoji = c.get('emoji') or ''
                    label = c.get('label', slug)
                    topic_count = len([n for n in c.get('topic_names', []) if n in all_topics])
                    mode = c.get('display_mode', 'buttons').capitalize()
                    prefix = f"{emoji} " if emoji else ""
                    lines.append(f"\u2022 {prefix}**{label}** \u2014 {topic_count} topics \u00b7 {mode}")
                embed.description = "\n".join(lines)
            return embed

        elif sp_type == "category_edit":
            cat_slug = sp["cat_slug"]
            cat_data = self.panel_data.get('categories', {}).get(cat_slug, {})
            all_topics = sp.get("topics", {})
            embed = discord.Embed(title=base + f"Edit Category: {cat_data.get('label', cat_slug)}", color=discord.Color.purple())
            emoji = cat_data.get('emoji') or '*None*'
            mode = cat_data.get('display_mode', 'buttons').capitalize()
            color = cat_data.get('button_color', 'primary').capitalize()
            topic_names = cat_data.get('topic_names', [])
            topic_lines = []
            for n in topic_names:
                t = all_topics.get(n, {})
                t_emoji = t.get('emoji') or ''
                t_label = t.get('label', n)
                topic_lines.append(f"\u2022 {t_emoji} {t_label}")
            topic_text = "\n".join(topic_lines) or "*None*"
            embed.description = (
                f"**Emoji:** {emoji}\n"
                f"**Ephemeral Display:** {mode}\n"
                f"**Button Color:** {color}\n\n"
                f"**Topics ({len(topic_names)}):**\n{topic_text}"
            )
            return embed

        elif sp_type == "category_topic_order":
            cat_slug = sp["cat_slug"]
            cat_data = self.panel_data.get('categories', {}).get(cat_slug, {})
            cat_topics = cat_data.get('topic_names', [])
            all_topics = sp.get("topics", {})
            lines = []
            for i, name in enumerate(cat_topics):
                t = all_topics.get(name, {})
                emoji = t.get('emoji') or ''
                label = t.get('label', name)
                lines.append(f"`{i+1}.` {emoji} {label}")
            embed = discord.Embed(title=base + f"Topic Order: {cat_data.get('label', cat_slug)}", color=discord.Color.purple())
            embed.description = (
                "Use the selects to reorder topics.\n"
                "**Move Up** shifts the selected topic up one position.\n"
                "**Move Down** shifts it down.\n\n"
                + ("\n".join(lines) or "*No topics*")
            )
            return embed

        elif sp_type == "category_delete":
            embed = discord.Embed(title=base + "Delete Category", color=discord.Color.red())
            embed.description = "Select a category to delete. This will not delete the topics inside it."
            return embed

        return discord.Embed(title="Configuration", color=discord.Color.purple())

    def _wizard_embed(self) -> discord.Embed:
        action = "Creating" if self.is_new else "Editing"
        embed = discord.Embed(title=f"{action} Panel: `{self.panel_data['name']}`", color=discord.Color.purple())
        embed.add_field(name="Title", value=self.panel_data.get('title'), inline=False)
        desc = self.panel_data.get('description')
        embed.add_field(name="Description", value=desc[:1024] if desc else "`Not set`", inline=False)
        channel_id = self.panel_data.get('channel_id')
        embed.add_field(name="Post Channel", value=f"<#{channel_id}>" if channel_id else "`Not set`", inline=True)
        embed.add_field(name="Display Mode", value=self.panel_data.get('display_mode', 'buttons').capitalize(), inline=True)
        image_url = self.panel_data.get('image_url')
        image_type = self.panel_data.get('image_type', 'banner')
        embed.add_field(name="Image", value=f"`{image_type.capitalize()}`" if image_url else "`Not set`", inline=True)
        attached = self.panel_data.get('topic_names', [])
        display_map = self.panel_data.get('topic_display_map', {})
        categories = self.panel_data.get('categories', {})
        order = self.panel_data.get('topic_order', [])

        # Determine categorized topics
        categorized = set()
        for cat_data in categories.values():
            for tn in cat_data.get('topic_names', []):
                if tn in attached:
                    categorized.add(tn)

        # Show in effective order
        ordered = [n for n in order if n in attached or (n.startswith("cat:") and n[4:] in categories)]
        for n in attached:
            if n not in ordered and n not in categorized:
                ordered.append(n)
        for slug in categories:
            if f"cat:{slug}" not in ordered:
                ordered.append(f"cat:{slug}")

        lines = []
        for item in ordered:
            if item.startswith("cat:"):
                slug = item[4:]
                cat = categories.get(slug)
                if cat:
                    emoji = cat.get('emoji') or '\U0001f4c1'
                    count = len(cat.get('topic_names', []))
                    lines.append(f"{emoji} **{cat.get('label', slug)}** ({count} topics)")
            elif item not in categorized:
                if self.panel_data.get('display_mode') == 'mixed':
                    kind = display_map.get(item, 'button')
                    icon = "\U0001f518" if kind == 'button' else "\U0001f53d"
                    lines.append(f"{icon} `{item}`")
                else:
                    lines.append(f"\u2022 `{item}`")

        topic_text = "\n".join(lines) or "`None`"
        embed.add_field(name=f"Attached Topics ({len(attached)})", value=topic_text[:1024], inline=False)
        return embed

    # --- Callbacks ---

    async def edit_text_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(PanelTextModal(self))

    async def set_channel_cb(self, interaction: discord.Interaction):
        self.subpage = {"type": "channel_picker"}
        await self._edit(interaction)

    async def set_image_cb(self, interaction: discord.Interaction):
        self.subpage = {"type": "image_picker"}
        await self._edit(interaction)

    async def manage_topics_cb(self, interaction: discord.Interaction):
        all_topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        if not all_topics:
            return await interaction.response.send_message("No topics created yet.", ephemeral=True, delete_after=10)
        self.subpage = {"type": "topic_manager", "topics": all_topics}
        await self._edit(interaction)

    async def toggle_display_cb(self, interaction: discord.Interaction):
        cycle = ['buttons', 'dropdown', 'mixed']
        current = self.panel_data.get('display_mode', 'buttons')
        idx = cycle.index(current) if current in cycle else 0
        self.panel_data['display_mode'] = cycle[(idx + 1) % len(cycle)]
        await self._edit(interaction)

    async def topic_layout_cb(self, interaction: discord.Interaction):
        all_topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        if not self.panel_data.get('topic_names'):
            return await interaction.response.send_message("Attach topics first.", ephemeral=True, delete_after=10)
        self.subpage = {"type": "topic_layout", "topics": all_topics}
        await self._edit(interaction)

    async def topic_order_cb(self, interaction: discord.Interaction):
        all_topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        if not self.panel_data.get('topic_names'):
            return await interaction.response.send_message("Attach topics first.", ephemeral=True, delete_after=10)
        self.subpage = {"type": "topic_order", "topics": all_topics}
        await self._edit(interaction)

    async def manage_categories_cb(self, interaction: discord.Interaction):
        all_topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        self.subpage = {"type": "category_manager", "topics": all_topics}
        await self._edit(interaction)

    async def _cat_create_cb(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateCategoryModal(self))

    async def _cat_delete_cb(self, interaction: discord.Interaction):
        all_topics = await _load_json(self.cog.bot, TOPICS_FILE, self.cog.topics_lock)
        self.subpage = {"type": "category_delete", "topics": all_topics}
        await self._edit(interaction)

    async def finish_cb(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self.panel_data.get('channel_id'):
            return await interaction.followup.send("Please set a post channel.", ephemeral=True)
        if not self.panel_data.get('topic_names'):
            return await interaction.followup.send("Please attach at least one topic.", ephemeral=True)
        try:
            panels = await _load_json(self.cog.bot, PANELS_FILE, self.cog.panels_lock)
            panels[self.panel_data['name']] = self.panel_data
            await _save_json(self.cog.bot, PANELS_FILE, panels, self.cog.panels_lock)
        except Exception as e:
            logger.error(f"Failed to save panel: {e}", exc_info=True)
            return await interaction.followup.send(f"An error occurred: `{e.__class__.__name__}`.", ephemeral=True)
        try:
            if self.wizard_message:
                await self.wizard_message.edit(content=f"Panel `{self.panel_data['name']}` saved.", embed=None, view=None)
            else:
                await self.original_interaction.edit_original_response(
                    content=f"Panel `{self.panel_data['name']}` saved.", embed=None, view=None
                )
        except discord.NotFound:
            pass
        self.stop()

    async def cancel_cb(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="Panel configuration cancelled.", embed=None, view=None)
        self.stop()


class PanelTextModal(discord.ui.Modal, title="Edit Panel Text"):
    title_input = discord.ui.TextInput(label="Embed Title", required=False)
    description_input = discord.ui.TextInput(label="Embed Description", style=discord.TextStyle.long, required=False)

    def __init__(self, wizard: PanelWizardView):
        super().__init__()
        self.wizard = wizard
        self.title_input.default = wizard.panel_data.get('title')
        self.description_input.default = wizard.panel_data.get('description')

    async def on_submit(self, itx: discord.Interaction):
        self.wizard.panel_data['title'] = self.title_input.value or None
        self.wizard.panel_data['description'] = self.description_input.value or None
        await self.wizard._refresh(itx)


class ImageUrlModal(discord.ui.Modal, title="Set Image URL"):
    image_url = discord.ui.TextInput(
        label="Image URL", placeholder="https://example.com/image.png", required=True
    )

    def __init__(self, wizard: PanelWizardView, image_type: str):
        super().__init__()
        self.wizard = wizard
        self.image_type = image_type
        self.image_url.default = wizard.panel_data.get('image_url')

    async def on_submit(self, itx: discord.Interaction):
        self.wizard.panel_data['image_url'] = self.image_url.value
        self.wizard.panel_data['image_type'] = self.image_type
        self.wizard.subpage = None
        await self.wizard._refresh(itx)


class CreateCategoryModal(discord.ui.Modal, title="Create Category"):
    slug_input = discord.ui.TextInput(
        label="Category ID (lowercase, no spaces)",
        placeholder="staff-questions", max_length=40
    )
    label_input = discord.ui.TextInput(
        label="Display Label",
        placeholder="Staff Questions", max_length=80
    )
    emoji_input = discord.ui.TextInput(
        label="Emoji (optional)", required=False, max_length=5
    )

    def __init__(self, wizard: PanelWizardView):
        super().__init__()
        self.wizard = wizard

    async def on_submit(self, itx: discord.Interaction):
        slug = self.slug_input.value.lower().replace(" ", "-")
        cats = self.wizard.panel_data.setdefault("categories", {})
        cats[slug] = {
            "label": self.label_input.value,
            "emoji": self.emoji_input.value or None,
            "display_mode": "buttons",
            "topic_names": [],
            "button_color": "primary"
        }
        # Add to topic_order
        order = self.wizard.panel_data.setdefault('topic_order', [])
        order.append(f"cat:{slug}")
        # Go to edit subpage to pick topics
        all_topics = await _load_json(self.wizard.cog.bot, TOPICS_FILE, self.wizard.cog.topics_lock)
        self.wizard.subpage = {"type": "category_edit", "cat_slug": slug, "topics": all_topics}
        await self.wizard._refresh(itx)


class EditCategoryModal(discord.ui.Modal, title="Edit Category"):
    label_input = discord.ui.TextInput(label="Display Label", max_length=80)
    emoji_input = discord.ui.TextInput(label="Emoji (optional)", required=False, max_length=5)

    def __init__(self, wizard: PanelWizardView, cat_slug: str):
        super().__init__()
        self.wizard = wizard
        self.cat_slug = cat_slug
        cat_data = wizard.panel_data.get('categories', {}).get(cat_slug, {})
        self.label_input.default = cat_data.get('label', '')
        self.emoji_input.default = cat_data.get('emoji', '')

    async def on_submit(self, itx: discord.Interaction):
        cat = self.wizard.panel_data.get('categories', {}).get(self.cat_slug)
        if cat:
            cat['label'] = self.label_input.value
            cat['emoji'] = self.emoji_input.value or None
        all_topics = await _load_json(self.wizard.cog.bot, TOPICS_FILE, self.wizard.cog.topics_lock)
        self.wizard.subpage = {"type": "category_edit", "cat_slug": self.cat_slug, "topics": all_topics}
        await self.wizard._refresh(itx)
