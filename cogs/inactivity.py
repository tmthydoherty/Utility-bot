import discord
from discord import app_commands, ui
from discord.ext import commands, tasks
import asyncio
from datetime import datetime, timedelta, timezone
import logging
import re

logger = logging.getLogger('betting_bot.inactivity')
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)

INACTIVITY_INVITE_LINK = "https://discord.gg/bettervibes"
DM_RATE_LIMIT_DELAY = 5.0
DM_CHUNK_SIZE = 25
DM_CHUNK_PAUSE = 60.0


# --- PERSISTENT ALERT VIEWS ---
def _make_alert_view(user_id: int, state: str = "initial", current_decision: str = None):
    """
    Build persistent button views for inactivity alerts.
    States:
      initial       - Kick / Snooze / Forget
      confirm_kick  - Confirm Kick / Don't Kick
      decided       - gear button (unless kicked)
      gear          - available options based on current_decision + cancel
    """
    view = ui.View(timeout=None)
    if state == "initial":
        view.add_item(ui.Button(label="Kick User", style=discord.ButtonStyle.danger, custom_id=f"inactivity:kick:{user_id}"))
        view.add_item(ui.Button(label="Snooze (Reset)", style=discord.ButtonStyle.primary, custom_id=f"inactivity:snooze:{user_id}"))
        view.add_item(ui.Button(label="Forget User", style=discord.ButtonStyle.secondary, custom_id=f"inactivity:forget:{user_id}"))
    elif state == "confirm_kick":
        view.add_item(ui.Button(label="Kick", style=discord.ButtonStyle.danger, custom_id=f"inactivity:confirmkick:{user_id}"))
        view.add_item(ui.Button(label="Don't Kick", style=discord.ButtonStyle.secondary, custom_id=f"inactivity:nokick:{user_id}"))
    elif state == "decided":
        if current_decision != "kicked":
            view.add_item(ui.Button(style=discord.ButtonStyle.secondary, emoji="\u2699\ufe0f", custom_id=f"inactivity:gear:{user_id}"))
    elif state == "gear":
        options = {"snoozed": ["kick", "forget"], "forgotten": ["kick", "snooze"]}.get(current_decision, ["kick", "snooze", "forget"])
        labels = {"kick": ("Kick User", discord.ButtonStyle.danger), "snooze": ("Snooze (Reset)", discord.ButtonStyle.primary), "forget": ("Forget User", discord.ButtonStyle.secondary)}
        for opt in options:
            lbl, style = labels[opt]
            cid = f"inactivity:{'kick' if opt == 'kick' else opt}:{user_id}"
            view.add_item(ui.Button(label=lbl, style=style, custom_id=cid))
        view.add_item(ui.Button(label="Cancel", style=discord.ButtonStyle.secondary, custom_id=f"inactivity:cancel:{user_id}"))
    return view


DECISION_LABELS = {
    "snoozed": ("Snooze User", discord.Color.blurple()),
    "forgotten": ("Forget User", discord.Color.dark_grey()),
    "kicked": ("Kick User", discord.Color.red()),
}

def _stamp_embed_decision(embed: discord.Embed, decision: str, admin: discord.Member, extra: str = None):
    """Update an existing alert embed to reflect the decision made."""
    label, color = DECISION_LABELS.get(decision, (decision.title(), discord.Color.dark_grey()))
    embed.color = color
    footer_text = f"{label} - {admin.display_name}"
    if extra:
        footer_text += f" \u2014 {extra}"
    embed.set_footer(text=footer_text)
    return embed


# --- INACTIVITY PANEL VIEW ---
class InactivityPanelView(ui.View):
    def __init__(self, cog, guild):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild = guild
        self.update_components()

    def update_components(self):
        self.clear_items()
        self.add_item(self.ConfigChannelSelect())
        self.add_item(self.ConfigRoleSelect())
        self.add_item(self.ConfigButton("Set Threshold", "threshold", discord.ButtonStyle.primary))
        self.add_item(self.ConfigButton("Set Period", "period", discord.ButtonStyle.secondary))
        self.add_item(self.TestButton())
        self.add_item(self.RepairButton())

    async def build_embed(self):
        row = await self.cog.db.fetch_one("SELECT * FROM inactivity_config WHERE guild_id = ?", (self.guild.id,))
        desc = "### Inactivity Settings\n\n"
        if row:
            chan = self.guild.get_channel(row['log_channel_id'])
            role = self.guild.get_role(row['highlight_role_id'])
            desc += f"**Log Channel:** {chan.mention if chan else 'Not Set'}\n"
            desc += f"**Highlight Role:** {role.mention if role else 'Not Set'}\n"
            desc += f"**Msg Threshold:** {row['msg_threshold']} messages\n"
            desc += f"**Time Period:** {row['period_days']} days\n"
        else:
            desc += "System not configured."
        return discord.Embed(description=desc, color=discord.Color.dark_grey())

    class ConfigChannelSelect(ui.ChannelSelect):
        def __init__(self): super().__init__(placeholder="Set Log Channel", channel_types=[discord.ChannelType.text], row=0)
        async def callback(self, interaction):
            await self.view.cog.db.execute("INSERT INTO inactivity_config (guild_id, log_channel_id) VALUES (?, ?) ON CONFLICT(guild_id) DO UPDATE SET log_channel_id = ?", (interaction.guild.id, self.values[0].id, self.values[0].id))
            await interaction.response.defer()
            embed = await self.view.build_embed()
            await interaction.edit_original_response(embed=embed, view=self.view)

    class ConfigRoleSelect(ui.RoleSelect):
        def __init__(self): super().__init__(placeholder="Set Highlight Role", row=1)
        async def callback(self, interaction):
            await self.view.cog.db.execute("INSERT INTO inactivity_config (guild_id, highlight_role_id) VALUES (?, ?) ON CONFLICT(guild_id) DO UPDATE SET highlight_role_id = ?", (interaction.guild.id, self.values[0].id, self.values[0].id))
            await interaction.response.defer()
            embed = await self.view.build_embed()
            await interaction.edit_original_response(embed=embed, view=self.view)

    class ConfigButton(ui.Button):
        def __init__(self, label, mode, style):
            super().__init__(label=label, style=style, row=2)
            self.mode = mode
        async def callback(self, interaction):
            await interaction.response.send_modal(InactivityPanelView.ConfigModal(interaction.client.get_cog("Inactivity"), self.mode, self.view))

    class TestButton(ui.Button):
        def __init__(self): super().__init__(label="Test Alert", style=discord.ButtonStyle.success, row=2)
        async def callback(self, interaction):
            row = await self.view.cog.db.fetch_one("SELECT * FROM inactivity_config WHERE guild_id = ?", (interaction.guild.id,))
            if not row or not row['log_channel_id']: return await interaction.response.send_message("Configure a Log Channel first.", ephemeral=True)
            log_channel = interaction.guild.get_channel(row['log_channel_id'])
            member = interaction.user
            embed = discord.Embed(title="TEST: Inactivity Alert", description="**0** messages in the last **30** days (Threshold: 5)", color=discord.Color.orange())
            embed.set_author(name=f"{member.display_name} ({member.name})", icon_url=member.display_avatar.url)
            embed.set_thumbnail(url=member.display_avatar.url)
            embed.add_field(name="Joined", value=member.joined_at.strftime("%Y-%m-%d") if member.joined_at else "Unknown", inline=True)
            embed.add_field(name="User ID", value=str(member.id), inline=True)
            try:
                await log_channel.send(content=member.mention, embed=embed, view=_make_alert_view(member.id, "initial"))
                await interaction.response.send_message(f"Sent test to {log_channel.mention}.", ephemeral=True)
            except:
                await interaction.response.send_message("Failed to send. Check bot permissions.", ephemeral=True)

    class RepairButton(ui.Button):
        def __init__(self): super().__init__(label="Repair Alerts", style=discord.ButtonStyle.secondary, emoji="\U0001f527", row=3)
        async def callback(self, interaction):
            try:
                cog = self.view.cog
                row = await cog.db.fetch_one("SELECT log_channel_id FROM inactivity_config WHERE guild_id = ?", (interaction.guild.id,))
                if not row or not row['log_channel_id']:
                    return await interaction.response.send_message("No log channel configured.", ephemeral=True)
                log_channel = interaction.guild.get_channel(row['log_channel_id'])
                if not log_channel:
                    return await interaction.response.send_message("Log channel not found.", ephemeral=True)
                await interaction.response.defer(ephemeral=True)
                to_repair = []
                async for message in log_channel.history(limit=500):
                    if message.author.id != cog.bot.user.id:
                        continue
                    if not message.embeds:
                        continue
                    embed = message.embeds[0]
                    if embed.title not in ("Inactivity Alert", "VIP Inactivity Alert", "TEST: Inactivity Alert"):
                        continue
                    user_id = None
                    for source in [message.content or "", embed.description or ""]:
                        match = re.search(r'<@!?(\d+)>', source)
                        if match:
                            user_id = int(match.group(1))
                            break
                    if not user_id:
                        for field in embed.fields:
                            if field.name == "User ID":
                                try: user_id = int(field.value)
                                except: pass
                    if not user_id:
                        continue
                    to_repair.append((message, embed, user_id))
                repaired = 0
                for message, embed, user_id in to_repair:
                    status_row = await cog.db.fetch_one("SELECT status FROM user_inactivity_status WHERE guild_id = ? AND user_id = ?", (interaction.guild.id, user_id))
                    if status_row and status_row['status'] in ('snoozed', 'forgotten', 'kicked'):
                        view = _make_alert_view(user_id, "decided", status_row['status'])
                    else:
                        view = _make_alert_view(user_id, "initial")
                    try:
                        content = f"<@{user_id}>"
                        await message.delete()
                        await log_channel.send(content=content, embed=embed, view=view)
                        repaired += 1
                        await asyncio.sleep(1.5)
                    except Exception as e:
                        logger.warning(f"[RepairAlerts] Failed to repair message {message.id}: {e}")
                await interaction.followup.send(f"Repaired **{repaired}** alert(s).", ephemeral=True)
            except Exception as e:
                logger.exception(f"[RepairAlerts] Button callback error: {e}")
                try:
                    await interaction.followup.send(f"Error: {e}", ephemeral=True)
                except:
                    pass

    class ConfigModal(ui.Modal):
        def __init__(self, cog, mode, parent_view):
            super().__init__(title=f"Set {mode.capitalize()}")
            self.cog = cog
            self.mode = mode
            self.parent_view = parent_view
            self.val = ui.TextInput(label="Value (Integer)", style=discord.TextStyle.short)
            self.add_item(self.val)

        async def on_submit(self, interaction):
            try:
                val = int(self.val.value)
                col = "msg_threshold" if self.mode == "threshold" else "period_days"
                await self.cog.db.execute(f"INSERT INTO inactivity_config (guild_id, {col}) VALUES (?, ?) ON CONFLICT(guild_id) DO UPDATE SET {col} = ?", (interaction.guild.id, val, val))
                embed = await self.parent_view.build_embed()
                await interaction.response.edit_message(embed=embed, view=self.parent_view)
            except ValueError:
                await interaction.response.send_message("Please enter a valid number.", ephemeral=True)


# --- MAIN COG ---
class Inactivity(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = None
        self.dm_queue: asyncio.Queue = asyncio.Queue()
        self._dm_worker_task = None

    async def cog_load(self):
        self._dm_worker_task = self.bot.loop.create_task(self._dm_worker())
        self.bot.loop.create_task(self._async_setup())

    async def _async_setup(self):
        await self.bot.wait_until_ready()
        tracker = self.bot.get_cog("UserTracker")
        if tracker:
            self.db = tracker.db
        else:
            logger.warning("[Inactivity] UserTracker cog not found, creating own DB connection.")
            from cogs.tracker import TrackingDB
            self.db = TrackingDB()
            await self.db.connect()
        self.check_inactivity_task.start()

    async def cog_unload(self):
        if self._dm_worker_task:
            self._dm_worker_task.cancel()
        self.check_inactivity_task.cancel()

    async def _dm_worker(self):
        """Background worker that drains the DM queue with rate-limit-safe delays."""
        sent_in_chunk = 0
        while True:
            try:
                user_id, guild_name = await self.dm_queue.get()
                try:
                    user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
                    if user:
                        msg = (
                            f"Hey! You've been removed from **{guild_name}** due to extended inactivity. "
                            f"No hard feelings \u2014 you're welcome back anytime!\n\n"
                            f"{INACTIVITY_INVITE_LINK}"
                        )
                        await user.send(msg)
                        sent_in_chunk += 1
                except discord.Forbidden:
                    logger.warning(f"[DM Queue] Cannot DM user {user_id} (DMs disabled).")
                except discord.HTTPException as e:
                    logger.warning(f"[DM Queue] Failed to DM user {user_id}: {e}")
                    if e.status == 429:
                        retry_after = getattr(e, 'retry_after', 30)
                        logger.warning(f"[DM Queue] Rate limited, backing off {retry_after}s.")
                        await asyncio.sleep(retry_after)
                except Exception as e:
                    logger.exception(f"[DM Queue] Unexpected error DMing user {user_id}: {e}")
                finally:
                    self.dm_queue.task_done()
                    if sent_in_chunk >= DM_CHUNK_SIZE:
                        logger.info(f"[DM Queue] Sent {sent_in_chunk} DMs, pausing {DM_CHUNK_PAUSE}s.")
                        await asyncio.sleep(DM_CHUNK_PAUSE)
                        sent_in_chunk = 0
                    else:
                        await asyncio.sleep(DM_RATE_LIMIT_DELAY)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    # --- COMMANDS ---
    @app_commands.command(name="inactivity_panel", description="Open the Inactivity Settings panel.")
    async def inactivity_panel_cmd(self, interaction: discord.Interaction):
        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("\u274c Administrator permission required.", ephemeral=True)
        await interaction.response.defer()
        view = InactivityPanelView(self, interaction.guild)
        embed = await view.build_embed()
        await interaction.followup.send(embed=embed, view=view)

    # --- LISTENERS ---
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        """Handle all persistent inactivity alert button presses."""
        if interaction.type != discord.InteractionType.component:
            return
        custom_id = interaction.data.get("custom_id", "")
        if not custom_id.startswith("inactivity:"):
            return
        parts = custom_id.split(":")
        if len(parts) != 3:
            return
        action, user_id_str = parts[1], parts[2]
        try:
            user_id = int(user_id_str)
        except ValueError:
            return

        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("\u274c Only administrators can use these controls.", ephemeral=True)

        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if not embed:
            return

        # --- KICK (shows confirmation first) ---
        if action == "kick":
            view = _make_alert_view(user_id, "confirm_kick")
            return await interaction.response.edit_message(view=view)

        # --- CONFIRM KICK ---
        if action == "confirmkick":
            member = interaction.guild.get_member(user_id)
            if member:
                try:
                    await member.kick(reason=f"Inactivity | admin:{interaction.user.id}")
                    self.dm_queue.put_nowait((user_id, interaction.guild.name))
                except Exception as e:
                    return await interaction.response.send_message(f"\u274c Failed to kick: {e}", ephemeral=True)
            await self.db.execute("INSERT OR REPLACE INTO user_inactivity_status (guild_id, user_id, status, snooze_until) VALUES (?, ?, 'kicked', 0)", (interaction.guild.id, user_id))
            updated = _stamp_embed_decision(embed, "kicked", interaction.user)
            view = _make_alert_view(user_id, "decided", "kicked")
            return await interaction.response.edit_message(embed=updated, view=view)

        # --- DON'T KICK (cancel confirmation, back to initial) ---
        if action == "nokick":
            status_row = await self.db.fetch_one("SELECT status FROM user_inactivity_status WHERE guild_id = ? AND user_id = ?", (interaction.guild.id, user_id))
            if status_row and status_row['status'] in ('snoozed', 'forgotten'):
                view = _make_alert_view(user_id, "decided", status_row['status'])
            else:
                view = _make_alert_view(user_id, "initial")
            return await interaction.response.edit_message(view=view)

        # --- SNOOZE ---
        if action == "snooze":
            row = await self.db.fetch_one("SELECT period_days FROM inactivity_config WHERE guild_id = ?", (interaction.guild.id,))
            days = row['period_days'] if row else 30
            snooze_until = int((datetime.now(timezone.utc) + timedelta(days=days)).timestamp())
            await self.db.execute("INSERT OR REPLACE INTO user_inactivity_status (guild_id, user_id, status, snooze_until) VALUES (?, ?, 'snoozed', ?)", (interaction.guild.id, user_id, snooze_until))
            updated = _stamp_embed_decision(embed, "snoozed", interaction.user, extra=f"reset for {days} days")
            view = _make_alert_view(user_id, "decided", "snoozed")
            return await interaction.response.edit_message(embed=updated, view=view)

        # --- FORGET ---
        if action == "forget":
            await self.db.execute("INSERT OR REPLACE INTO user_inactivity_status (guild_id, user_id, status, snooze_until) VALUES (?, ?, 'forgotten', 0)", (interaction.guild.id, user_id))
            updated = _stamp_embed_decision(embed, "forgotten", interaction.user)
            view = _make_alert_view(user_id, "decided", "forgotten")
            return await interaction.response.edit_message(embed=updated, view=view)

        # --- GEAR (expand options) ---
        if action == "gear":
            status_row = await self.db.fetch_one("SELECT status FROM user_inactivity_status WHERE guild_id = ? AND user_id = ?", (interaction.guild.id, user_id))
            current = status_row['status'] if status_row else "alerted"
            view = _make_alert_view(user_id, "gear", current)
            return await interaction.response.edit_message(view=view)

        # --- CANCEL (back to decided state with gear) ---
        if action == "cancel":
            status_row = await self.db.fetch_one("SELECT status FROM user_inactivity_status WHERE guild_id = ? AND user_id = ?", (interaction.guild.id, user_id))
            current = status_row['status'] if status_row else "alerted"
            view = _make_alert_view(user_id, "decided", current)
            return await interaction.response.edit_message(view=view)

    # --- TASKS ---
    @tasks.loop(hours=24)
    async def check_inactivity_task(self):
        try:
            await self.bot.wait_until_ready()
            logger.info("Inactivity check task started.")
            for guild in self.bot.guilds:
                try:
                    row = await self.db.fetch_one("SELECT * FROM inactivity_config WHERE guild_id = ?", (guild.id,))
                    if not row or not row['log_channel_id']:
                        logger.info(f"[Inactivity] Guild '{guild.name}': no config, skipping.")
                        continue
                    log_channel = guild.get_channel(row['log_channel_id'])
                    if not log_channel:
                        logger.warning(f"[Inactivity] Guild '{guild.name}': log channel {row['log_channel_id']} not found.")
                        continue
                    highlight_role = guild.get_role(row['highlight_role_id']) if row['highlight_role_id'] else None
                    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=row['period_days'])).timestamp())
                    status_rows = await self.db.fetch_all("SELECT user_id, status, snooze_until FROM user_inactivity_status WHERE guild_id = ?", (guild.id,))
                    statuses = {r['user_id']: {'status': r['status'], 'snooze_until': r['snooze_until']} for r in status_rows}
                    count_rows = await self.db.fetch_all("SELECT user_id, count(*) as c FROM message_logs WHERE guild_id = ? AND timestamp > ? GROUP BY user_id", (guild.id, cutoff_ts))
                    msg_counts = {r['user_id']: r['c'] for r in count_rows}
                    updates_to_clear = []
                    alerts_to_send = []
                    logger.info(f"[Inactivity] Guild '{guild.name}': checking {len(guild.members)} members (threshold={row['msg_threshold']}, period={row['period_days']}d).")
                    skip_too_new = skip_threshold = skip_status = 0
                    for member in guild.members:
                        if member.bot: continue
                        if not member.joined_at: continue
                        count = msg_counts.get(member.id, 0)
                        if count >= row['msg_threshold']:
                            if member.id in statuses and statuses[member.id]['status'] in ('alerted', 'snoozed'):
                                updates_to_clear.append(member.id)
                            skip_threshold += 1
                            continue
                        join_ts = int(member.joined_at.timestamp())
                        if join_ts > cutoff_ts:
                            skip_too_new += 1
                            continue
                        status_info = statuses.get(member.id)
                        if status_info:
                            if status_info['status'] in ('forgotten', 'kicked'):
                                skip_status += 1
                                continue
                            if status_info['status'] == 'snoozed' and datetime.now(timezone.utc).timestamp() < status_info['snooze_until']:
                                skip_status += 1
                                continue
                            if status_info['status'] == 'alerted':
                                skip_status += 1
                                continue
                        alerts_to_send.append((member, count))
                    logger.info(f"[Inactivity] Guild '{guild.name}': skipped {skip_threshold} (met threshold), {skip_too_new} (joined too recently), {skip_status} (status).")
                    if updates_to_clear:
                        async with self.db.transaction() as conn:
                            for uid in updates_to_clear:
                                await conn.execute("DELETE FROM user_inactivity_status WHERE guild_id = ? AND user_id = ?", (guild.id, uid))
                    logger.info(f"[Inactivity] Guild '{guild.name}': {len(alerts_to_send)} alerts to send, {len(updates_to_clear)} statuses to clear.")
                    for member, count in alerts_to_send:
                        color = discord.Color.red() if (highlight_role and highlight_role in member.roles) else discord.Color.orange()
                        title = "VIP Inactivity Alert" if (highlight_role and highlight_role in member.roles) else "Inactivity Alert"
                        embed = discord.Embed(title=title, description=f"**{count}** messages in the last **{row['period_days']}** days (Threshold: {row['msg_threshold']})", color=color)
                        embed.set_author(name=f"{member.display_name} ({member.name})", icon_url=member.display_avatar.url)
                        embed.set_thumbnail(url=member.display_avatar.url)
                        embed.add_field(name="Joined", value=member.joined_at.strftime("%Y-%m-%d"), inline=True)
                        embed.add_field(name="User ID", value=str(member.id), inline=True)
                        await log_channel.send(content=member.mention, embed=embed, view=_make_alert_view(member.id, "initial"))
                        await self.db.execute("INSERT OR REPLACE INTO user_inactivity_status (guild_id, user_id, status, snooze_until) VALUES (?, ?, 'alerted', 0)", (guild.id, member.id))
                        await asyncio.sleep(2)
                except Exception as e:
                    logger.exception(f"[Inactivity] Guild '{guild.name}' check failed: {e}")
                await asyncio.sleep(1)
        except Exception as e:
            await self.bot.error_reporter.report("Inactivity", f"check_inactivity_task: {e}")


async def setup(bot):
    await bot.add_cog(Inactivity(bot))
