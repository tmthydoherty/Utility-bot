import discord
from discord.ext import commands
from discord import app_commands
import json
import os
from datetime import datetime
import logging

logger = logging.getLogger('bot_main')
CONFIG_FILE = "utility_config.json"


class DeleteThreadView(discord.ui.View):
    def __init__(self, op_id: int):
        super().__init__(timeout=None)
        self.op_id = op_id

    @discord.ui.button(label="Delete thread", style=discord.ButtonStyle.danger, custom_id="delete_thread_btn")
    async def delete_thread_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_admin = False
        if isinstance(interaction.client, commands.Bot) and hasattr(interaction.client, 'is_bot_admin'):
            is_admin = interaction.client.is_bot_admin(interaction.user)

        if interaction.user.id != self.op_id and not is_admin and not interaction.user.guild_permissions.manage_threads:
            await interaction.response.send_message("Only the original poster or a moderator can delete this thread.", ephemeral=True)
            return

        await interaction.response.send_message("Deleting thread...", ephemeral=True)
        await interaction.channel.delete()


# ─── Media Channel Views ───

class MediaChannelSelectView(discord.ui.View):
    """Channel select for adding or replacing a media channel."""
    def __init__(self, cog: 'Utility', edit_index: int = None):
        super().__init__(timeout=120)
        self.cog = cog
        self.edit_index = edit_index

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        placeholder="Select a channel...",
        channel_types=[discord.ChannelType.text, discord.ChannelType.forum, discord.ChannelType.news],
        min_values=1,
        max_values=1,
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        guild_id = str(interaction.guild_id)
        channel = select.values[0]

        if guild_id not in self.cog.config:
            self.cog.config[guild_id] = {}
        if "media_channels" not in self.cog.config[guild_id]:
            self.cog.config[guild_id]["media_channels"] = []

        channels = self.cog.config[guild_id]["media_channels"]

        if self.edit_index is not None and 0 <= self.edit_index < len(channels):
            old_id = channels[self.edit_index]
            channels[self.edit_index] = channel.id
            self.cog.save_config()
            embed = discord.Embed(
                title="Media Channel Updated",
                description=f"<#{old_id}> → <#{channel.id}>",
                color=discord.Color.green()
            )
        else:
            if channel.id in channels:
                embed = discord.Embed(
                    title="Already Configured",
                    description=f"<#{channel.id}> is already a media channel.",
                    color=discord.Color.orange()
                )
            else:
                channels.append(channel.id)
                self.cog.save_config()
                embed = discord.Embed(
                    title="Media Channel Added",
                    description=f"<#{channel.id}> is now a media-only channel. Regular text messages will be deleted.",
                    color=discord.Color.green()
                )

        await interaction.response.edit_message(embed=embed, view=None)


class ManageMediaOverview(discord.ui.View):
    """Overview of media channels with add/edit/remove controls."""
    def __init__(self, cog: 'Utility', guild_id: str, guild: discord.Guild):
        super().__init__(timeout=120)
        self.cog = cog
        self.guild_id = guild_id
        self._selected_index = None

        channels = self.cog.config.get(guild_id, {}).get("media_channels", [])
        if channels:
            options = []
            for i, ch_id in enumerate(channels):
                ch_obj = guild.get_channel(ch_id)
                name = f"#{ch_obj.name}" if ch_obj else f"Unknown ({ch_id})"
                options.append(discord.SelectOption(label=name[:100], value=str(i)))
            self._rule_select = discord.ui.Select(
                placeholder="Select a media channel...",
                options=options[:25],
                row=0,
            )
            self._rule_select.callback = self._on_select
            self.add_item(self._rule_select)

    async def _on_select(self, interaction: discord.Interaction):
        self._selected_index = int(self._rule_select.values[0])
        await interaction.response.defer()

    @discord.ui.button(label="Add Channel", style=discord.ButtonStyle.success, row=1)
    async def add_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = MediaChannelSelectView(self.cog)
        embed = discord.Embed(
            title="Add Media Channel",
            description="Select a channel to make media-only:",
            color=discord.Color.orange()
        )
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Edit Selected", style=discord.ButtonStyle.primary, row=1)
    async def edit_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._selected_index is None:
            await interaction.response.send_message("Select a channel from the dropdown first.", ephemeral=True)
            return
        view = MediaChannelSelectView(self.cog, edit_index=self._selected_index)
        embed = discord.Embed(
            title="Edit Media Channel",
            description="Select the new channel to replace it with:",
            color=discord.Color.orange()
        )
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Remove Selected", style=discord.ButtonStyle.danger, row=1)
    async def remove_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._selected_index is None:
            await interaction.response.send_message("Select a channel from the dropdown first.", ephemeral=True)
            return
        channels = self.cog.config.get(self.guild_id, {}).get("media_channels", [])
        if 0 <= self._selected_index < len(channels):
            removed = channels.pop(self._selected_index)
            self.cog.save_config()
            embed = discord.Embed(
                title="Media Channel Removed",
                description=f"<#{removed}> is no longer a media-only channel.",
                color=discord.Color.red()
            )
            await interaction.response.edit_message(embed=embed, view=None)
        else:
            await interaction.response.send_message("Channel not found.", ephemeral=True)


# ─── Reaction Rule Views ───

class ManageReactionsChannelSelect(discord.ui.View):
    """Step 1: Select a channel/thread for the reaction rule."""
    def __init__(self, cog: 'Utility', edit_index: int = None):
        super().__init__(timeout=120)
        self.cog = cog
        self.edit_index = edit_index

    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        placeholder="Select a channel or thread...",
        channel_types=[
            discord.ChannelType.text,
            discord.ChannelType.forum,
            discord.ChannelType.public_thread,
            discord.ChannelType.private_thread,
            discord.ChannelType.news,
            discord.ChannelType.news_thread,
        ],
        min_values=1,
        max_values=1,
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        channel = select.values[0]
        action = "Edit Rule" if self.edit_index is not None else "Add Rule"
        view = ReactionRuleTypeView(self.cog, channel.id, self.edit_index)
        embed = discord.Embed(
            title=f"Manage Reactions — {action}",
            description=f"**Channel:** <#{channel.id}>\n\nChoose when reactions should be removed:",
            color=discord.Color.orange()
        )
        await interaction.response.edit_message(embed=embed, view=view)


class ReactionRuleTypeView(discord.ui.View):
    """Step 2: Choose the rule type."""
    def __init__(self, cog: 'Utility', channel_id: int, edit_index: int = None):
        super().__init__(timeout=120)
        self.cog = cog
        self.channel_id = channel_id
        self.edit_index = edit_index

    def _save_rule(self, guild_id: str, rule_data: dict):
        if guild_id not in self.cog.config:
            self.cog.config[guild_id] = {}
        if "reaction_rules" not in self.cog.config[guild_id]:
            self.cog.config[guild_id]["reaction_rules"] = []

        rules = self.cog.config[guild_id]["reaction_rules"]
        if self.edit_index is not None and 0 <= self.edit_index < len(rules):
            rules[self.edit_index] = rule_data
        else:
            rules.append(rule_data)
        self.cog.save_config()

    @discord.ui.button(label="All Messages", style=discord.ButtonStyle.primary)
    async def rule_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild_id)
        self._save_rule(guild_id, {"channel_id": self.channel_id, "type": "all"})
        action = "Updated" if self.edit_index is not None else "Added"
        embed = discord.Embed(
            title=f"Rule {action}",
            description=f"Reactions will now be removed from **all messages** in <#{self.channel_id}>.",
            color=discord.Color.green()
        )
        await interaction.response.edit_message(embed=embed, view=None)

    @discord.ui.button(label="Messages with @Role Mention", style=discord.ButtonStyle.primary)
    async def rule_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ReactionRoleSelectView(self.cog, self.channel_id, self.edit_index)
        embed = discord.Embed(
            title="Manage Reactions",
            description=f"**Channel:** <#{self.channel_id}>\n\nSelect the role(s) — reactions will be removed from messages that mention these roles.",
            color=discord.Color.orange()
        )
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Messages from a User", style=discord.ButtonStyle.primary)
    async def rule_user(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ReactionUserSelectView(self.cog, self.channel_id, self.edit_index)
        embed = discord.Embed(
            title="Manage Reactions",
            description=f"**Channel:** <#{self.channel_id}>\n\nSelect the user(s) — reactions will be removed from their messages.",
            color=discord.Color.orange()
        )
        await interaction.response.edit_message(embed=embed, view=view)


class ReactionRoleSelectView(discord.ui.View):
    """Step 3a: Select roles for the role-mention rule."""
    def __init__(self, cog: 'Utility', channel_id: int, edit_index: int = None):
        super().__init__(timeout=120)
        self.cog = cog
        self.channel_id = channel_id
        self.edit_index = edit_index

    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="Select role(s)...",
        min_values=1,
        max_values=10,
    )
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        guild_id = str(interaction.guild_id)
        if guild_id not in self.cog.config:
            self.cog.config[guild_id] = {}
        if "reaction_rules" not in self.cog.config[guild_id]:
            self.cog.config[guild_id]["reaction_rules"] = []

        role_ids = [r.id for r in select.values]
        role_mentions = ", ".join(r.mention for r in select.values)
        rule_data = {"channel_id": self.channel_id, "type": "role_mention", "role_ids": role_ids}

        rules = self.cog.config[guild_id]["reaction_rules"]
        if self.edit_index is not None and 0 <= self.edit_index < len(rules):
            rules[self.edit_index] = rule_data
            action = "Updated"
        else:
            rules.append(rule_data)
            action = "Added"
        self.cog.save_config()

        embed = discord.Embed(
            title=f"Rule {action}",
            description=f"Reactions will be removed from messages in <#{self.channel_id}> that mention: {role_mentions}",
            color=discord.Color.green()
        )
        await interaction.response.edit_message(embed=embed, view=None)


class ReactionUserSelectView(discord.ui.View):
    """Step 3b: Select users for the from-user rule."""
    def __init__(self, cog: 'Utility', channel_id: int, edit_index: int = None):
        super().__init__(timeout=120)
        self.cog = cog
        self.channel_id = channel_id
        self.edit_index = edit_index

    @discord.ui.select(
        cls=discord.ui.UserSelect,
        placeholder="Select user(s)...",
        min_values=1,
        max_values=10,
    )
    async def user_select(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        guild_id = str(interaction.guild_id)
        if guild_id not in self.cog.config:
            self.cog.config[guild_id] = {}
        if "reaction_rules" not in self.cog.config[guild_id]:
            self.cog.config[guild_id]["reaction_rules"] = []

        user_ids = [u.id for u in select.values]
        user_mentions = ", ".join(u.mention for u in select.values)
        rule_data = {"channel_id": self.channel_id, "type": "from_user", "user_ids": user_ids}

        rules = self.cog.config[guild_id]["reaction_rules"]
        if self.edit_index is not None and 0 <= self.edit_index < len(rules):
            rules[self.edit_index] = rule_data
            action = "Updated"
        else:
            rules.append(rule_data)
            action = "Added"
        self.cog.save_config()

        embed = discord.Embed(
            title=f"Rule {action}",
            description=f"Reactions will be removed from messages by {user_mentions} in <#{self.channel_id}>.",
            color=discord.Color.green()
        )
        await interaction.response.edit_message(embed=embed, view=None)


class ManageReactionsOverview(discord.ui.View):
    """Overview of reaction rules with add/edit/remove controls."""
    def __init__(self, cog: 'Utility', guild_id: str, guild: discord.Guild):
        super().__init__(timeout=120)
        self.cog = cog
        self.guild_id = guild_id
        self._selected_index = None

        rules = self.cog.config.get(guild_id, {}).get("reaction_rules", [])
        if rules:
            options = []
            for i, rule in enumerate(rules):
                ch_obj = guild.get_channel(rule["channel_id"])
                ch_name = f"#{ch_obj.name}" if ch_obj else f"#{rule['channel_id']}"

                if rule["type"] == "all":
                    label = f"{ch_name} — All messages"
                    desc = "Remove all reactions"
                elif rule["type"] == "role_mention":
                    label = f"{ch_name} — Role mention"
                    role_names = []
                    for rid in rule["role_ids"]:
                        r = guild.get_role(rid)
                        role_names.append(f"@{r.name}" if r else str(rid))
                    desc = ", ".join(role_names)
                elif rule["type"] == "from_user":
                    label = f"{ch_name} — From user"
                    desc = f"{len(rule['user_ids'])} user(s)"
                else:
                    label = f"{ch_name} — Unknown"
                    desc = "Unknown rule type"

                options.append(discord.SelectOption(
                    label=label[:100],
                    description=desc[:100],
                    value=str(i)
                ))

            self._rule_select = discord.ui.Select(
                placeholder="Select a rule...",
                options=options[:25],
                row=0,
            )
            self._rule_select.callback = self._on_select
            self.add_item(self._rule_select)

    async def _on_select(self, interaction: discord.Interaction):
        self._selected_index = int(self._rule_select.values[0])
        await interaction.response.defer()

    @discord.ui.button(label="Add Rule", style=discord.ButtonStyle.success, row=1)
    async def add_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = ManageReactionsChannelSelect(self.cog)
        embed = discord.Embed(
            title="Manage Reactions — Add Rule",
            description="Select a channel or thread:",
            color=discord.Color.orange()
        )
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Edit Selected", style=discord.ButtonStyle.primary, row=1)
    async def edit_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._selected_index is None:
            await interaction.response.send_message("Select a rule from the dropdown first.", ephemeral=True)
            return
        view = ManageReactionsChannelSelect(self.cog, edit_index=self._selected_index)
        embed = discord.Embed(
            title="Manage Reactions — Edit Rule",
            description="Select the channel or thread for this rule:",
            color=discord.Color.orange()
        )
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Remove Selected", style=discord.ButtonStyle.danger, row=1)
    async def remove_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._selected_index is None:
            await interaction.response.send_message("Select a rule from the dropdown first.", ephemeral=True)
            return
        rules = self.cog.config.get(self.guild_id, {}).get("reaction_rules", [])
        if 0 <= self._selected_index < len(rules):
            removed = rules.pop(self._selected_index)
            self.cog.save_config()
            ch_id = removed["channel_id"]
            if removed["type"] == "all":
                desc = f"Removed: all reactions in <#{ch_id}>"
            elif removed["type"] == "role_mention":
                roles = ", ".join(f"<@&{r}>" for r in removed["role_ids"])
                desc = f"Removed: reactions on messages mentioning {roles} in <#{ch_id}>"
            else:
                users = ", ".join(f"<@{u}>" for u in removed["user_ids"])
                desc = f"Removed: reactions on messages from {users} in <#{ch_id}>"
            embed = discord.Embed(title="Rule Removed", description=desc, color=discord.Color.red())
            await interaction.response.edit_message(embed=embed, view=None)
        else:
            await interaction.response.send_message("Rule not found.", ephemeral=True)


# ─── Panel View ───

class UtilityPanelView(discord.ui.View):
    def __init__(self, cog: 'Utility'):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Manage Media Channels", style=discord.ButtonStyle.primary, custom_id="manage_media_btn")
    async def manage_media(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.client.is_bot_admin(interaction.user):
            await interaction.response.send_message("You do not have permission to use this.", ephemeral=True)
            return

        guild_id = str(interaction.guild_id)
        channels = self.cog.config.get(guild_id, {}).get("media_channels", [])

        if channels:
            lines = [f"`{i+1}.` <#{ch_id}>" for i, ch_id in enumerate(channels)]
            summary = "\n".join(lines)
        else:
            summary = "*No media channels configured yet.*"

        embed = discord.Embed(
            title="Manage Media Channels",
            description=f"Media-only channels delete non-media messages and auto-create threads for posts.\n\n**Current Channels:**\n{summary}",
            color=discord.Color.orange()
        )
        view = ManageMediaOverview(self.cog, guild_id, interaction.guild)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Manage Reactions", style=discord.ButtonStyle.primary, custom_id="manage_reactions_btn")
    async def manage_reactions(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.client.is_bot_admin(interaction.user):
            await interaction.response.send_message("You do not have permission to use this.", ephemeral=True)
            return

        guild_id = str(interaction.guild_id)
        guild = interaction.guild
        rules = self.cog.config.get(guild_id, {}).get("reaction_rules", [])

        if rules:
            lines = []
            for i, rule in enumerate(rules, 1):
                ch = f"<#{rule['channel_id']}>"
                if rule["type"] == "all":
                    lines.append(f"`{i}.` {ch} — All messages")
                elif rule["type"] == "role_mention":
                    roles = ", ".join(f"<@&{r}>" for r in rule["role_ids"])
                    lines.append(f"`{i}.` {ch} — Messages mentioning {roles}")
                elif rule["type"] == "from_user":
                    users = ", ".join(f"<@{u}>" for u in rule["user_ids"])
                    lines.append(f"`{i}.` {ch} — Messages from {users}")
            summary = "\n".join(lines)
        else:
            summary = "*No reaction rules configured yet.*"

        embed = discord.Embed(
            title="Manage Reactions",
            description=f"Reaction rules automatically remove reactions from messages matching your criteria.\n\n**Current Rules:**\n{summary}",
            color=discord.Color.orange()
        )
        view = ManageReactionsOverview(self.cog, guild_id, guild)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


# ─── Cog ───

class Utility(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = self.load_config()
        self._migrate_config()

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r") as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        return {}

    def _migrate_config(self):
        """Migrate old singular media_channel to plural media_channels list."""
        changed = False
        for guild_id, data in self.config.items():
            if "media_channel" in data:
                old = data.pop("media_channel")
                if "media_channels" not in data:
                    data["media_channels"] = []
                if old not in data["media_channels"]:
                    data["media_channels"].append(int(old))
                changed = True
        if changed:
            self.save_config()

    def save_config(self):
        with open(CONFIG_FILE, "w") as f:
            json.dump(self.config, f, indent=4)

    @app_commands.command(name="utility_panel", description="Opens the utility configuration panel.")
    @app_commands.default_permissions(administrator=True)
    async def utility_panel(self, interaction: discord.Interaction):
        if not self.bot.is_bot_admin(interaction.user):
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        embed = discord.Embed(
            title="Utility Configuration Panel",
            description="Use the buttons below to configure utility settings for this server.",
            color=discord.Color.blue()
        )
        view = UtilityPanelView(self)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    def has_media(self, message: discord.Message) -> bool:
        if message.attachments:
            for attachment in message.attachments:
                if attachment.content_type and (attachment.content_type.startswith("image/") or attachment.content_type.startswith("video/")):
                    return True
        return False

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if not payload.guild_id:
            return
        if payload.user_id == self.bot.user.id:
            return

        guild_id = str(payload.guild_id)
        rules = self.config.get(guild_id, {}).get("reaction_rules", [])
        if not rules:
            return

        channel_id = payload.channel_id
        matching_rules = [r for r in rules if r["channel_id"] == channel_id]

        # If no direct match, check if we're in a thread whose parent has a rule
        if not matching_rules:
            channel = self.bot.get_channel(channel_id)
            # If cached but missing parent_id, or not cached at all, fetch from API
            if isinstance(channel, discord.Thread) and not channel.parent_id:
                channel = None
            if not channel:
                try:
                    channel = await self.bot.fetch_channel(channel_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                    logger.debug(f"[Utility] Could not fetch channel {channel_id} for reaction rule: {e}")
                    return

            parent_id = getattr(channel, 'parent_id', None)
            if parent_id:
                matching_rules = [r for r in rules if r["channel_id"] == parent_id]
            if not matching_rules:
                return
        else:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                try:
                    channel = await self.bot.fetch_channel(channel_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    return

        message = None
        for rule in matching_rules:
            if rule["type"] == "all":
                try:
                    if message is None:
                        message = await channel.fetch_message(payload.message_id)
                    await message.remove_reaction(payload.emoji, discord.Object(id=payload.user_id))
                except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                    pass
                return

            if message is None:
                try:
                    message = await channel.fetch_message(payload.message_id)
                except (discord.NotFound, discord.HTTPException):
                    return

            should_remove = False

            if rule["type"] == "role_mention":
                mentioned_role_ids = set(message.raw_role_mentions)
                if mentioned_role_ids & set(rule["role_ids"]):
                    should_remove = True
            elif rule["type"] == "from_user":
                if message.author.id in rule["user_ids"]:
                    should_remove = True

            if should_remove:
                try:
                    await message.remove_reaction(payload.emoji, discord.Object(id=payload.user_id))
                except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                    pass
                return

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        guild_id = str(message.guild.id)
        media_channels = self.config.get(guild_id, {}).get("media_channels", [])
        if not media_channels:
            return

        # Convert all to int for safe comparison
        media_channel_ids = {int(ch) for ch in media_channels}

        # Allow messages in threads whose parent is a media channel
        if isinstance(message.channel, discord.Thread) and message.channel.parent_id in media_channel_ids:
            return

        if message.channel.id not in media_channel_ids:
            return

        # Enforce media-only
        if not self.has_media(message):
            try:
                await message.delete()
            except discord.Forbidden:
                logger.warning(f"Failed to delete message in {message.channel.name}. Bot is missing 'Manage Messages' permission.")
            except discord.NotFound:
                pass
            return

        # Valid media — create a thread
        try:
            date_str = datetime.now().strftime("%Y-%m-%d")
            thread_name = f"{message.author.display_name} - {date_str}"
            thread = await message.create_thread(
                name=thread_name,
                auto_archive_duration=60
            )
            view = DeleteThreadView(op_id=message.author.id)
            await thread.send(
                content=f"{message.author.mention} This thread was created as a comment section. You can use the button below to delete it if you'd like.",
                view=view
            )
        except Exception as e:
            logger.error(f"Failed to create thread for media message: {e}")


async def setup(bot: commands.Bot):
    await bot.add_cog(Utility(bot))
