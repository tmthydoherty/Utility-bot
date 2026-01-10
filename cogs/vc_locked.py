import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import time
import logging
import sys
import os

# --- IMPORT FIX ---
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import vc_locked_shared as shared

class VC(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.trigger_name = shared.TRIGGER_NAME
        self.active_vcs = {}
        self.hub_channel_id = None
        self.guild_id = None
        self.tasks = {} 
        self.cleanup_task = None
        self.trigger_validation_task = None 
        self.vc_text_channels = {} 
        
        # State Saving
        self._save_lock = asyncio.Lock()
        self._save_pending = False 
        self._save_task = None
        
        # Hub Messaging
        self._hub_message_locks = {}
        
        # Hub Renaming & Rate Limits
        self._hub_name_lock = asyncio.Lock()
        self._last_hub_update = 0
        self._hub_name_edits = [] 
        self._hub_rename_queue = {} 
        self._hub_rename_task = None 
        
        # Cooldowns
        self.knock_cooldown = commands.CooldownMapping.from_cooldown(1, 300, commands.BucketType.user)
        self.create_cooldown = commands.CooldownMapping.from_cooldown(1, 60, commands.BucketType.user)
        
        # Internal Tracking
        self.creating_vcs = set()
        self.vc_creation_cooldowns = {}
        self.knock_tracking = {} 
        self.pending_knocks = {}  # vc_id: [user_ids]
        self.last_knock_ping = {}  # vc_id: timestamp
        self._hub_update_cooldowns = {}
        self._transfer_locks = {} 
        self._name_update_debounce = {} 
        
        # FIX: Track accepted knocks for auto-cleanup
        self.accepted_knocks = {}  # vc_id -> {user_id: {'msg_id': int, 'thread_id': int, 'task': Task}}
        
        # FIX: Lock for active_vcs modifications
        self._active_vcs_lock = asyncio.Lock()
        
        # FIX: Track cleanup in progress to prevent double cleanup
        self._cleanup_in_progress = set()
        
        shared.logger.info("VC Cog initialized")

    async def cog_load(self):
        shared.logger.info("VC Cog loading...")
        try:
            await shared.init_db()
            
            guild_id_str = await shared.get_config('guild_id')
            if guild_id_str:
                self.guild_id = int(guild_id_str)
            elif self.bot.guilds:
                self.guild_id = self.bot.guilds[0].id
                await shared.set_config('guild_id', self.guild_id)

            # Restore state BEFORE starting background tasks
            await self.restore_state()
            
            # FIX: Re-register persistent views with message validation
            view_count = 0
            for vc_id, data in self.active_vcs.items():
                if data.get('knock_mgmt_msg_id') and data.get('thread_id'):
                    # Verify message exists before registering view
                    thread = self.bot.get_channel(data['thread_id'])
                    if thread:
                        try:
                            await thread.fetch_message(data['knock_mgmt_msg_id'])
                            view = shared.KnockManagementView(self.bot, self, data['owner_id'], vc_id)
                            self.bot.add_view(view, message_id=data['knock_mgmt_msg_id'])
                            view_count += 1
                        except discord.NotFound:
                            # Message deleted, clear reference
                            data['knock_mgmt_msg_id'] = None
                            shared.logger.warning(f"Knock management message missing for VC {vc_id}, cleared reference")
                        except Exception as e:
                            shared.logger.error(f"Failed to validate knock mgmt message for VC {vc_id}: {e}")
                    else:
                        # Thread missing, clear references
                        data['thread_id'] = None
                        data['knock_mgmt_msg_id'] = None
                        shared.logger.warning(f"Thread missing for VC {vc_id}, cleared references")
            
            # Global persistent views
            self.bot.add_view(shared.AdminPanelView(self.bot))
            self.bot.add_view(shared.RulesView(self.bot)) 
            
            # FIX: Restore hub rename queue from database
            await self._restore_hub_rename_queue()
            
            # Background tasks - start AFTER restoration complete
            self.cleanup_task = asyncio.create_task(self.periodic_cleanup())
            self.trigger_validation_task = asyncio.create_task(self.validate_trigger_channel())
            
            # FIX: Start hub rename queue processor if there are pending renames
            if self._hub_rename_queue:
                self._hub_rename_task = asyncio.create_task(self._process_hub_rename_queue())
            
            # FIX: Force hub name update for all guilds after restoration
            for guild in self.bot.guilds:
                try:
                    await self.update_hub_name(guild, force=True)
                except Exception as e:
                    shared.logger.error(f"Failed initial hub rename for {guild.id}: {e}")
            
            shared.logger.info(f"VC Cog loaded successfully. {len(self.active_vcs)} VCs restored, {view_count} views registered")
        except Exception as e:
            shared.logger.error(f"Critical error during cog_load: {e}", exc_info=True)
            raise

    async def _restore_hub_rename_queue(self):
        """Restore pending hub renames from database"""
        try:
            queue_data = await shared.get_config('hub_rename_queue')
            if queue_data:
                import json
                self._hub_rename_queue = json.loads(queue_data)
                shared.logger.info(f"Restored {len(self._hub_rename_queue)} pending hub renames")
        except Exception as e:
            shared.logger.error(f"Failed to restore hub rename queue: {e}")

    async def _persist_hub_rename_queue(self):
        """Save pending hub renames to database"""
        try:
            import json
            await shared.set_config('hub_rename_queue', json.dumps(self._hub_rename_queue))
        except Exception as e:
            shared.logger.error(f"Failed to persist hub rename queue: {e}")

    async def restore_state(self):
        shared.logger.info("Restoring VC state from database...")
        try:
            self.active_vcs = await shared.load_active_vcs()

            to_delete = []
            owner_map = {}
            guilds_to_update = set()

            for vc_id, data in list(self.active_vcs.items()):
                # FIX: Invalid state (ghost+unlocked is impossible)
                if data.get('ghost', False) and data.get('unlocked', False):
                    shared.logger.warning(f"VC {vc_id} has invalid state (ghost+unlocked), fixing")
                    data['unlocked'] = False

                try:
                    # === VERIFY VIA API AT STARTUP ===
                    # At bot startup, cache may not be populated. Use API to verify.
                    guild_id = data.get('guild_id')
                    guild = self.bot.get_guild(guild_id) if guild_id else None

                    if not guild:
                        # Try to get guild from bot's guilds list
                        for g in self.bot.guilds:
                            shared.logger.debug(f"RESTORE: Trying guild {g.id} for VC {vc_id}")
                            guild = g
                            break

                    if not guild:
                        shared.logger.warning(f"RESTORE: No guild found for VC {vc_id}, skipping (will try self-heal later)")
                        continue

                    # Verify channel via API, not cache
                    vc, member_count = await shared.verify_channel_exists(self.bot, guild, vc_id)

                    # Can't verify - skip for safety, self-heal will handle later
                    if vc == "FORBIDDEN" or vc == "ERROR":
                        shared.logger.warning(f"RESTORE: Cannot verify VC {vc_id}, skipping (will try self-heal later)")
                        continue

                    # Channel confirmed deleted - mark for cleanup
                    if not vc:
                        shared.logger.info(f"RESTORE: VC {vc_id} confirmed deleted via API, marking for cleanup")
                        to_delete.append(vc_id)
                        continue

                    oid = data['owner_id']
                    if oid in owner_map:
                        shared.logger.warning(f"Duplicate owner detected during restore: {oid}")
                        existing_vc_id = owner_map[oid]
                        # Verify existing VC via API too
                        existing_vc, existing_count = await shared.verify_channel_exists(self.bot, guild, existing_vc_id)

                        if not existing_vc or (isinstance(existing_vc, discord.VoiceChannel) and member_count > existing_count):
                            to_delete.append(existing_vc_id)
                            owner_map[oid] = vc_id
                        else:
                            to_delete.append(vc_id)
                            continue
                    else:
                        owner_map[oid] = vc_id

                    guilds_to_update.add(guild.id)
                    if 'guild_id' not in data:
                        data['guild_id'] = guild.id
                    owner = guild.get_member(oid)

                    if not owner:
                        shared.logger.warning(f"Owner {oid} not found for VC {vc_id}")
                        # Don't delete the VC if it has members - transfer instead
                        if member_count > 0 and vc.members:
                            shared.logger.info(f"RESTORE: VC {vc_id} has {member_count} members but no owner, will transfer")
                            await self.transfer_ownership(vc, vc.members[0])
                            continue
                        else:
                            to_delete.append(vc_id)
                            try: await vc.delete()
                            except Exception: pass
                            continue

                    hub_id_str = await shared.get_config(f'hub_channel_id_{guild.id}')
                    hub = guild.get_channel(int(hub_id_str)) if hub_id_str else None

                    # FIX: CRITICAL - Cleanup messages for VCs that are unlocked or ghosted
                    if (data.get('unlocked', False) or data.get('ghost', False)) and data.get('message_id'):
                        shared.logger.info(f"Cleaning up hub message for VC {vc_id} (unlocked={data.get('unlocked')}, ghost={data.get('ghost')})")
                        if hub:
                            try:
                                msg = hub.get_partial_message(data['message_id'])
                                await msg.delete()
                                shared.logger.info(f"Deleted stale hub message {data['message_id']} for VC {vc_id}")
                            except discord.NotFound:
                                shared.logger.debug(f"Hub message {data['message_id']} already deleted")
                            except Exception as e:
                                shared.logger.error(f"Failed to delete hub message: {e}")
                        data['message_id'] = None

                    # FIX: Ensure locked VCs have hub messages
                    if hub and data.get('message_id'):
                        try:
                            # Re-verify main message exists
                            await hub.fetch_message(data['message_id'])
                            
                            # Clean duplicate knock messages
                            messages = [m async for m in hub.history(limit=50) if m.author.id == self.bot.user.id]
                            for m in messages:
                                if m.id != data['message_id'] and m.components:
                                    for row in m.components:
                                        for child in row.children:
                                            if getattr(child, 'custom_id', None) == f"knock:{vc_id}":
                                                try: 
                                                    await m.delete()
                                                    shared.logger.info(f"Deleted duplicate knock message for VC {vc_id}")
                                                except Exception: pass
                        except discord.NotFound: 
                            shared.logger.warning(f"Hub message {data['message_id']} not found, will recreate")
                            data['message_id'] = None 

                    # FIX: Re-register views with message verification
                    if data.get('message_id') and hub and not data.get('unlocked', False) and not data.get('ghost', False):
                        try:
                            # Verify message still exists before registering view
                            await hub.fetch_message(data['message_id'])
                            view = shared.HubEntryView(self.bot, self, data['owner_id'], vc_id)
                            self.bot.add_view(view, message_id=data['message_id'])
                            shared.logger.debug(f"Re-registered view for VC {vc_id}")
                        except discord.NotFound:
                            shared.logger.warning(f"Hub message {data['message_id']} disappeared, recreating")
                            data['message_id'] = None
                            await self.create_hub_message(vc)
                    elif not data.get('unlocked', False) and not data.get('ghost', False):
                        # Create missing hub message for locked VCs
                        await self.create_hub_message(vc)
                    
                    # FIX: Handle threads - DELETE if exists (never archive)
                    if data.get('thread_id'):
                        thread = self.bot.get_channel(data['thread_id'])
                        if not thread: 
                            shared.logger.warning(f"Thread {data['thread_id']} for VC {vc_id} not found")
                            data['thread_id'] = None
                            data['knock_mgmt_msg_id'] = None
                        # Thread exists - keep it active, unarchive if needed
                        elif isinstance(thread, discord.Thread) and thread.archived:
                            try:
                                await thread.edit(archived=False)
                                shared.logger.info(f"Unarchived thread {thread.id} for VC {vc_id}")

                                # FIX: Verify knock management message exists after unarchiving
                                if data.get('knock_mgmt_msg_id'):
                                    try:
                                        await thread.fetch_message(data['knock_mgmt_msg_id'])
                                    except discord.NotFound:
                                        # Message missing, recreate it
                                        owner = guild.get_member(data['owner_id'])
                                        if owner:
                                            embed = shared.create_knock_management_embed(owner, [], guild, data)
                                            view = shared.KnockManagementView(self.bot, self, owner.id, vc_id)
                                            msg = await thread.send(content=owner.mention, embed=embed, view=view)
                                            self.bot.add_view(view, message_id=msg.id)
                                            data['knock_mgmt_msg_id'] = msg.id
                                            shared.logger.info(f"Recreated knock management message for VC {vc_id}")
                            except Exception as e:
                                shared.logger.error(f"Failed to unarchive thread: {e}")
                                # Thread is archived and can't unarchive - delete it
                                try:
                                    await thread.delete()
                                    data['thread_id'] = None
                                    data['knock_mgmt_msg_id'] = None
                                except Exception:
                                    pass

                    # FIX: Reconcile permissions to match database state
                    vc_data = self.active_vcs.get(vc_id)
                    if vc_data:
                        expected_connect = vc_data.get('unlocked', False) or vc_data.get('is_basic', False)
                        actual_perms = vc.overwrites_for(guild.default_role)

                        if actual_perms.connect != expected_connect:
                            shared.logger.warning(f"VC {vc_id} permissions mismatch: DB={'unlocked' if expected_connect else 'locked'}, Discord={actual_perms.connect}")
                            # Fix permissions to match database
                            try:
                                await self.safe_set_permissions(vc, guild.default_role, connect=expected_connect)
                                shared.logger.info(f"Reconciled permissions for VC {vc_id}")
                            except Exception as e:
                                shared.logger.error(f"Failed to reconcile permissions for VC {vc_id}: {e}")

                    # Map category text channels for VC visibility tracking
                    if vc.category:
                        for tc in vc.category.text_channels:
                            self.vc_text_channels[tc.id] = vc_id

                    self.pending_knocks[vc_id] = []
                    self.last_knock_ping[vc_id] = 0

                    # FIX: Schedule cleanup with guild context preserved (using API-verified member_count)
                    if member_count == 0:
                        shared.logger.info(f"RESTORE: VC {vc_id} is empty (verified via API), scheduling cleanup")
                        self._schedule_cleanup_with_context(vc_id, guild.id)
                    elif owner not in vc.members:
                        shared.logger.info(f"RESTORE: Owner not in VC {vc_id}, initiating transfer")
                        await self.transfer_or_cleanup(vc, data['owner_id'])

                except Exception as e:
                    shared.logger.error(f"Error restoring VC {vc_id}: {e}", exc_info=True)
            
            # Clean up deleted VCs
            for vid in to_delete:
                await self.cleanup_vc_by_id(vid)
            
            await self.save_state()
            shared.logger.info(f"State restoration complete. {len(self.active_vcs)} VCs active.")
            
        except Exception as e:
            shared.logger.error(f"Critical error during restore_state: {e}", exc_info=True)
            raise

    async def cog_unload(self):
        shared.logger.info("Unloading VC Cog...")

        # Cancel background tasks
        if self.cleanup_task:
            self.cleanup_task.cancel()
        if self.trigger_validation_task:
            self.trigger_validation_task.cancel()
        if self._hub_rename_task and not self._hub_rename_task.done():
            self._hub_rename_task.cancel()

        # FIX: Persist hub rename queue before unload
        await self._persist_hub_rename_queue()

        # Cancel all VC-specific tasks
        for vc_id, tasks in self.tasks.items():
            for t in tasks.values():
                if not t.done():
                    t.cancel()

        # FIX: Cancel accepted knock cleanup tasks
        for vc_id, users in self.accepted_knocks.items():
            for user_id, data in users.items():
                if data.get('task') and not data['task'].done():
                    data['task'].cancel()

        shared.logger.info("VC Cog unloaded.")

    async def _re_register_all_views(self):
        """Re-register all persistent views after bot reconnection"""
        shared.logger.info("Re-registering all persistent views after reconnection...")

        view_count = 0
        knock_mgmt_count = 0
        hub_msg_count = 0

        try:
            # Re-register knock management views (in private threads)
            for vc_id, data in list(self.active_vcs.items()):
                if data.get('knock_mgmt_msg_id') and data.get('thread_id'):
                    thread = self.bot.get_channel(data['thread_id'])
                    if thread:
                        try:
                            # Verify message still exists
                            await thread.fetch_message(data['knock_mgmt_msg_id'])
                            view = shared.KnockManagementView(self.bot, self, data['owner_id'], vc_id)
                            self.bot.add_view(view, message_id=data['knock_mgmt_msg_id'])
                            knock_mgmt_count += 1
                        except discord.NotFound:
                            shared.logger.warning(f"Knock mgmt message {data['knock_mgmt_msg_id']} missing for VC {vc_id}")
                            data['knock_mgmt_msg_id'] = None
                        except Exception as e:
                            shared.logger.error(f"Failed to re-register knock mgmt view for VC {vc_id}: {e}")
                    else:
                        shared.logger.warning(f"Thread {data['thread_id']} missing for VC {vc_id}")
                        data['thread_id'] = None
                        data['knock_mgmt_msg_id'] = None

                # Re-register hub entry views (knock buttons in hub channel)
                if data.get('message_id') and not data.get('unlocked', False) and not data.get('ghost', False):
                    vc = self.bot.get_channel(vc_id)
                    if vc and vc.guild:
                        hub_id = await shared.get_config(f"hub_channel_id_{vc.guild.id}")
                        if hub_id:
                            hub = vc.guild.get_channel(int(hub_id))
                            if hub:
                                try:
                                    # Verify message still exists
                                    await hub.fetch_message(data['message_id'])
                                    view = shared.HubEntryView(self.bot, self, data['owner_id'], vc_id)
                                    self.bot.add_view(view, message_id=data['message_id'])
                                    hub_msg_count += 1
                                except discord.NotFound:
                                    shared.logger.warning(f"Hub message {data['message_id']} missing for VC {vc_id}")
                                    data['message_id'] = None
                                    # Try to recreate the message
                                    await self.create_hub_message(vc)
                                except Exception as e:
                                    shared.logger.error(f"Failed to re-register hub view for VC {vc_id}: {e}")

            # Re-register global persistent views
            self.bot.add_view(shared.AdminPanelView(self.bot))
            self.bot.add_view(shared.RulesView(self.bot))
            view_count = knock_mgmt_count + hub_msg_count + 2

            shared.logger.info(f"Re-registered {view_count} views ({knock_mgmt_count} knock mgmt, {hub_msg_count} hub, 2 global)")
            await self.save_state()

        except Exception as e:
            shared.logger.error(f"Error during view re-registration: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_ready(self):
        """Called when bot is ready - re-register views after reconnection"""
        shared.logger.info("VC Cog: Bot ready event received")

        try:
            # Re-register all views to handle bot reconnections
            await self._re_register_all_views()

            # Restart hub rename queue processor if needed
            if self._hub_rename_queue and (not self._hub_rename_task or self._hub_rename_task.done()):
                shared.logger.info("Restarting hub rename queue processor after reconnection")
                self._hub_rename_task = asyncio.create_task(self._process_hub_rename_queue())

            # Perform health check on all active VCs
            await self._health_check_all_vcs()

        except Exception as e:
            shared.logger.error(f"Error in on_ready: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_resume(self):
        """Called when bot resumes connection after disconnect"""
        shared.logger.info("VC Cog: Bot resume event received - re-registering views")

        try:
            # Re-register all views after gateway resume
            await self._re_register_all_views()

            # Restart hub rename queue processor if needed
            if self._hub_rename_queue and (not self._hub_rename_task or self._hub_rename_task.done()):
                shared.logger.info("Restarting hub rename queue processor after resume")
                self._hub_rename_task = asyncio.create_task(self._process_hub_rename_queue())

        except Exception as e:
            shared.logger.error(f"Error in on_resume: {e}", exc_info=True)

    async def _health_check_all_vcs(self):
        """Perform health check on all active VCs - uses API verification"""
        shared.logger.info("Performing health check on all active VCs...")

        issues_found = 0
        issues_fixed = 0

        try:
            for vc_id, data in list(self.active_vcs.items()):
                guild_id = data.get('guild_id')
                guild = self.bot.get_guild(guild_id) if guild_id else None

                if not guild:
                    shared.logger.warning(f"Health check: No guild for VC {vc_id}, skipping")
                    continue

                # === VERIFY VIA API ===
                vc, member_count = await shared.verify_channel_exists(self.bot, guild, vc_id)

                # Can't verify - skip to be safe
                if vc == "FORBIDDEN" or vc == "ERROR":
                    shared.logger.debug(f"Health check: Cannot verify VC {vc_id}, skipping")
                    continue

                # VC confirmed deleted
                if not vc:
                    shared.logger.warning(f"Health check: VC {vc_id} confirmed deleted via API, cleaning up")
                    await self.cleanup_vc_by_id(vc_id)
                    issues_found += 1
                    issues_fixed += 1
                    continue

                # Check thread validity
                if data.get('thread_id'):
                    thread = self.bot.get_channel(data['thread_id'])
                    if not thread:
                        shared.logger.warning(f"Health check: Thread {data['thread_id']} missing for VC {vc_id}")
                        data['thread_id'] = None
                        data['knock_mgmt_msg_id'] = None
                        issues_found += 1
                        issues_fixed += 1
                    elif isinstance(thread, discord.Thread) and thread.archived:
                        shared.logger.warning(f"Health check: Thread {data['thread_id']} archived for VC {vc_id}, deleting")
                        await self._delete_thread(data['thread_id'])
                        data['thread_id'] = None
                        data['knock_mgmt_msg_id'] = None
                        issues_found += 1
                        issues_fixed += 1

                # Check hub message for locked VCs
                if not data.get('unlocked', False) and not data.get('ghost', False) and not data.get('is_basic', False):
                    if not data.get('message_id'):
                        shared.logger.warning(f"Health check: Locked VC {vc_id} missing hub message, recreating")
                        await self.create_hub_message(vc)
                        issues_found += 1
                        issues_fixed += 1
                    else:
                        # Verify hub message still exists
                        hub_id = await shared.get_config(f"hub_channel_id_{guild.id}")
                        if hub_id:
                            hub = guild.get_channel(int(hub_id))
                            if hub:
                                try:
                                    await hub.fetch_message(data['message_id'])
                                except discord.NotFound:
                                    shared.logger.warning(f"Health check: Hub message {data['message_id']} missing for VC {vc_id}, recreating")
                                    data['message_id'] = None
                                    await self.create_hub_message(vc)
                                    issues_found += 1
                                    issues_fixed += 1

                # Check owner still in guild
                owner = guild.get_member(data['owner_id'])
                if not owner:
                    shared.logger.warning(f"Health check: Owner {data['owner_id']} not in guild for VC {vc_id}")
                    if member_count > 0:
                        await self.transfer_ownership(vc, vc.members[0])
                        issues_found += 1
                        issues_fixed += 1
                    else:
                        await self.cleanup_vc_by_id(vc_id)
                        issues_found += 1
                        issues_fixed += 1
                    continue

                # Check permission state matches database
                expected_connect = data.get('unlocked', False) or data.get('is_basic', False)
                actual_perms = vc.overwrites_for(guild.default_role)
                if actual_perms.connect != expected_connect:
                    shared.logger.warning(f"Health check: VC {vc_id} permissions mismatch, fixing")
                    await self.safe_set_permissions(vc, guild.default_role, connect=expected_connect)
                    issues_found += 1
                    issues_fixed += 1

            if issues_found > 0:
                shared.logger.info(f"Health check complete: {issues_found} issues found, {issues_fixed} fixed")
                await self.save_state()
            else:
                shared.logger.info("Health check complete: No issues found")

        except Exception as e:
            shared.logger.error(f"Error during health check: {e}", exc_info=True)

    async def save_state(self):
        self._save_pending = True
        if self._save_task and not self._save_task.done():
            return True
        self._save_task = asyncio.create_task(self._do_save())
        return True

    async def _do_save(self):
        await asyncio.sleep(1)
        async with self._save_lock:
            if not self._save_pending: 
                return
            self._save_pending = False
            try:
                await shared.save_multiple_vcs(self.active_vcs)
                shared.logger.debug("State saved successfully")
            except Exception as e:
                shared.logger.error(f"Failed to save state: {e}", exc_info=True)

    def get_vc_data(self, voice_id):
        return self.active_vcs.get(voice_id)

    # --- HELPERS ---

    async def validate_owner(self, interaction, vc_id):
        if vc_id not in self.active_vcs:
            await interaction.response.send_message("❌ This VC no longer exists.", ephemeral=True)
            return False
        vc_data = self.active_vcs[vc_id]
        if interaction.user.id != vc_data['owner_id']:
            await interaction.response.send_message("❌ Only the VC owner can do this.", ephemeral=True)
            return False
        vc = interaction.guild.get_channel(vc_id)
        if not vc:
            await self.cleanup_vc_by_id(vc_id)
            await interaction.response.send_message("❌ VC no longer exists.", ephemeral=True)
            return False
        return True

    async def reconnect_vc(self, voice_channel):
        """
        Reconnect an orphaned VC back to tracking.
        Called when we find a VC that exists on Discord but isn't in active_vcs.
        This is the core of the self-healing system.
        """
        vc_id = voice_channel.id
        guild = voice_channel.guild

        # Skip if already tracked
        if vc_id in self.active_vcs:
            shared.logger.debug(f"RECONNECT: VC {vc_id} already tracked, skipping")
            return True

        shared.logger.info(f"RECONNECT: Attempting to reconnect orphaned VC {vc_id} ({voice_channel.name})")

        # Determine owner from permissions (whoever has MANAGE_CHANNELS overwrite)
        owner = None
        for target, overwrite in voice_channel.overwrites.items():
            if isinstance(target, discord.Member) and overwrite.manage_channels:
                owner = target
                shared.logger.debug(f"RECONNECT: Found owner {owner.id} from MANAGE_CHANNELS permission")
                break

        # Fallback: first non-bot member in the VC
        if not owner:
            for member in voice_channel.members:
                if not member.bot:
                    owner = member
                    shared.logger.debug(f"RECONNECT: Using first non-bot member {owner.id} as owner")
                    break

        if not owner:
            shared.logger.warning(f"RECONNECT: Cannot reconnect VC {vc_id} - no owner found and no members")
            return False

        # Determine if locked based on name prefix and permissions
        is_locked = voice_channel.name.startswith("🔒 ")
        default_perms = voice_channel.overwrites_for(guild.default_role)
        is_unlocked = default_perms.connect is not False  # None or True means unlocked

        shared.logger.info(f"RECONNECT: VC {vc_id} - is_locked={is_locked}, is_unlocked={is_unlocked}")

        # Restore tracking entry
        async with self._active_vcs_lock:
            self.active_vcs[vc_id] = {
                'owner_id': owner.id,
                'message_id': None,
                'knock_mgmt_msg_id': None,
                'thread_id': None,
                'ghost': False,
                'unlocked': is_unlocked,
                'is_basic': not is_locked and is_unlocked,
                'bans': [],
                'mute_knock_pings': False,
                'guild_id': guild.id
            }

        # Initialize tracking dicts
        self.pending_knocks[vc_id] = []
        self.last_knock_ping[vc_id] = 0

        await self.save_state()

        # For locked VCs, recreate hub message and restore settings thread
        if is_locked and not is_unlocked:
            shared.logger.info(f"RECONNECT: Restoring hub message and thread for locked VC {vc_id}")

            # Create hub message
            await self.create_hub_message(voice_channel)

            # Try to find existing thread first, only create new if needed
            hub_id = await shared.get_config(f"hub_channel_id_{guild.id}")
            if hub_id:
                hub = guild.get_channel(int(hub_id))
                if hub:
                    try:
                        clean_name = shared.sanitize_name(owner.display_name, owner.id)[:20]
                        expected_thread_name = f"🔒 {clean_name}'s VC Settings"
                        thread = None

                        # Search for existing thread belonging to this owner
                        try:
                            async for t in hub.archived_threads(limit=50):
                                if t.name == expected_thread_name or (owner.mention in t.name):
                                    # Found archived thread - unarchive it
                                    await t.edit(archived=False)
                                    thread = t
                                    shared.logger.info(f"RECONNECT: Found and unarchived existing thread {t.id}")
                                    break

                            if not thread:
                                # Check active threads
                                for t in hub.threads:
                                    if t.name == expected_thread_name:
                                        thread = t
                                        shared.logger.info(f"RECONNECT: Found existing active thread {t.id}")
                                        break
                        except Exception as e:
                            shared.logger.debug(f"RECONNECT: Error searching for existing thread: {e}")

                        # Create new thread only if none found
                        if not thread:
                            shared.logger.info(f"RECONNECT: No existing thread found, creating new one silently")
                            perms = hub.permissions_for(guild.me)

                            if not perms.create_private_threads or not perms.manage_threads:
                                thread = await hub.create_thread(
                                    name=expected_thread_name,
                                    auto_archive_duration=1440
                                )
                            else:
                                thread = await hub.create_thread(
                                    name=expected_thread_name,
                                    type=discord.ChannelType.private_thread,
                                    auto_archive_duration=1440,
                                    invitable=False
                                )

                            # Silently add user to new thread
                            try:
                                await thread.add_user(owner)
                            except discord.Forbidden:
                                pass  # Silent - no notification needed

                        # Send new settings embed silently (no ping/mention)
                        view = shared.KnockManagementView(self.bot, self, owner.id, vc_id)
                        embed = shared.create_knock_management_embed(owner, [], guild, self.active_vcs[vc_id])
                        knock_msg = await thread.send(embed=embed, view=view)  # No content/mention
                        self.bot.add_view(view, message_id=knock_msg.id)

                        self.active_vcs[vc_id]['knock_mgmt_msg_id'] = knock_msg.id
                        self.active_vcs[vc_id]['thread_id'] = thread.id
                        await self.save_state()

                        shared.logger.info(f"RECONNECT: Restored thread {thread.id} for VC {vc_id}")
                    except Exception as e:
                        shared.logger.error(f"RECONNECT: Failed to restore thread for VC {vc_id}: {e}")

        shared.logger.info(f"RECONNECT: Successfully reconnected VC {vc_id} owned by {owner.id} ({owner.display_name})")
        return True

    async def cleanup_vc_by_id(self, vc_id):
        """Clean up all data associated with a VC - IMPROVED with comprehensive fallbacks"""
        # FIX: Prevent double cleanup
        if vc_id in self._cleanup_in_progress:
            shared.logger.debug(f"Cleanup already in progress for VC {vc_id}")
            return

        # === VERIFY BEFORE DESTROY ===
        # Before any cleanup, verify via API that the channel is actually gone or empty
        vc_data = self.active_vcs.get(vc_id)
        if vc_data:
            guild_id = vc_data.get('guild_id')
            guild = self.bot.get_guild(guild_id) if guild_id else None

            if guild:
                channel, member_count = await shared.verify_channel_exists(self.bot, guild, vc_id)

                # If we can't verify (API error or forbidden), abort cleanup to be safe
                if channel == "FORBIDDEN" or channel == "ERROR":
                    shared.logger.warning(f"ABORT CLEANUP: Cannot verify VC {vc_id}, aborting to prevent data loss")
                    return

                # If channel exists with members, reconnect instead of cleanup!
                if channel and member_count > 0:
                    shared.logger.info(
                        f"ABORT CLEANUP: VC {vc_id} still exists with {member_count} members! "
                        f"Reconnecting instead of cleaning up."
                    )
                    await self.reconnect_vc(channel)
                    return

                shared.logger.debug(f"VERIFY: Cleanup confirmed for VC {vc_id} (channel={'exists but empty' if channel else 'deleted'})")

        self._cleanup_in_progress.add(vc_id)
        guild = None  # Track guild for final hub update

        try:
            shared.logger.info(f"Cleaning up VC {vc_id}")

            # FIX: Get guild BEFORE deleting data for hub rename
            vc_data = self.active_vcs.get(vc_id)
            if vc_data:
                guild_id = vc_data.get('guild_id')
                if guild_id:
                    guild = self.bot.get_guild(guild_id)
                if not guild:
                    vc = self.bot.get_channel(vc_id)
                    if vc and hasattr(vc, 'guild'):
                        guild = vc.guild

            # CRITICAL: Delete hub message first (with error handling)
            try:
                await self.delete_hub_message(vc_id)
            except Exception as e:
                shared.logger.error(f"Error deleting hub message for VC {vc_id}: {e}")
                # Continue cleanup even if message deletion fails

            # FIX: Delete thread (never archive)
            if vc_data and vc_data.get('thread_id'):
                try:
                    await self._delete_thread(vc_data['thread_id'])
                except Exception as e:
                    shared.logger.error(f"Error deleting thread for VC {vc_id}: {e}")
                    # Continue cleanup

            # Clean up database and memory
            try:
                await shared.delete_vc_data(vc_id)

                async with self._active_vcs_lock:
                    self.active_vcs.pop(vc_id, None)

                # Clean up tracking dicts
                for tc_id, vid in list(self.vc_text_channels.items()):
                    if vid == vc_id:
                        del self.vc_text_channels[tc_id]

                self.knock_tracking.pop(vc_id, None)
                self.pending_knocks.pop(vc_id, None)
                self.last_knock_ping.pop(vc_id, None)
                self._transfer_locks.pop(vc_id, None)
                self._hub_message_locks.pop(vc_id, None)

                # Cancel and clean up tasks
                if vc_id in self.tasks:
                    for t in self.tasks[vc_id].values():
                        if not t.done():
                            t.cancel()
                    del self.tasks[vc_id]

                # Clean up accepted knocks
                if vc_id in self.accepted_knocks:
                    for user_id in list(self.accepted_knocks[vc_id].keys()):
                        try:
                            await self.cleanup_accepted_knock(vc_id, user_id)
                        except Exception as e:
                            shared.logger.error(f"Error cleaning up accepted knock for user {user_id}: {e}")
                    self.accepted_knocks.pop(vc_id, None)

            except Exception as e:
                shared.logger.error(f"Error cleaning up VC data for {vc_id}: {e}", exc_info=True)

        finally:
            # CRITICAL: Always remove from cleanup_in_progress
            self._cleanup_in_progress.discard(vc_id)

            # CRITICAL: Always try to update hub name after cleanup (even if cleanup had errors)
            if guild:
                try:
                    shared.logger.debug(f"Updating hub name after cleanup of VC {vc_id}")
                    await self.update_hub_name(guild, force=True)
                except Exception as e:
                    shared.logger.error(f"Failed to update hub name after cleanup of VC {vc_id}: {e}")
                    # Schedule a retry via the rename queue as fallback
                    try:
                        active_locked = [v for v in self.active_vcs.values()
                                       if v.get('guild_id') == guild.id
                                       and not v.get('ghost', False)
                                       and not v.get('unlocked', False)
                                       and not v.get('is_basic', False)]
                        if not active_locked:
                            expected_name = await shared.get_config(f"idle_name_{guild.id}", "🔑-join-locked-vcs")
                        else:
                            expected_name = "🔑-join-locked-vcs"
                        self._hub_rename_queue[guild.id] = expected_name
                        if not self._hub_rename_task or self._hub_rename_task.done():
                            self._hub_rename_task = asyncio.create_task(self._process_hub_rename_queue())
                    except Exception as fallback_error:
                        shared.logger.error(f"Fallback hub rename queueing also failed: {fallback_error}")

    async def _delete_thread(self, thread_id):
        """Delete a thread - never archive"""
        try:
            thread = self.bot.get_channel(thread_id)
            if thread:
                await thread.delete()
                shared.logger.info(f"Deleted thread {thread_id}")
        except discord.NotFound:
            shared.logger.debug(f"Thread {thread_id} already deleted")
        except Exception as e:
            shared.logger.error(f"Failed to delete thread {thread_id}: {e}")

    def _schedule_cleanup_with_context(self, vc_id, guild_id):
        """Schedule cleanup task with preserved guild context"""
        if vc_id in self.tasks and self.tasks[vc_id].get('cleanup'):
            self.tasks[vc_id]['cleanup'].cancel()
        if vc_id not in self.tasks: 
            self.tasks[vc_id] = {}
        self.tasks[vc_id]['cleanup'] = asyncio.create_task(
            self._monitor_empty_vc_with_context(vc_id, guild_id)
        )

    def schedule_cleanup_task(self, voice_channel):
        """Schedule cleanup task for a voice channel"""
        vc_id = voice_channel.id
        guild_id = voice_channel.guild.id if voice_channel.guild else None
        
        if not guild_id:
            vc_data = self.active_vcs.get(vc_id)
            if vc_data:
                guild_id = vc_data.get('guild_id')
        
        if vc_id in self.tasks and self.tasks[vc_id].get('cleanup'):
            self.tasks[vc_id]['cleanup'].cancel()
        if vc_id not in self.tasks: 
            self.tasks[vc_id] = {}
        self.tasks[vc_id]['cleanup'] = asyncio.create_task(
            self._monitor_empty_vc_with_context(vc_id, guild_id)
        )

    async def safe_set_permissions(self, channel, target, **permissions):
        """Safely set permissions on a channel for a target"""
        # FIX: Validate target is a valid Member or Role
        if target is None:
            shared.logger.debug("safe_set_permissions called with None target")
            return False
        
        # FIX: Check if target is a Member (not just a User)
        if isinstance(target, discord.User) and not isinstance(target, discord.Member):
            shared.logger.debug(f"Target {target.id} is User not Member, cannot set permissions")
            return False
        
        try:
            await channel.set_permissions(target, **permissions)
            return True
        except discord.Forbidden: 
            shared.logger.debug(f"Forbidden: Cannot set permissions for {target.id} on {channel.id}")
            return False
        except discord.HTTPException as e:
            shared.logger.debug(f"HTTP error setting permissions for {target.id}: {e}")
            return False
        except Exception as e:
            shared.logger.debug(f"Permission error for {target.id}: {e}")
            return False

    async def safe_edit_channel(self, channel, **kwargs):
        try: 
            await channel.edit(**kwargs)
            return True
        except discord.HTTPException as e:
            if e.status == 429:
                shared.logger.warning(f"Rate limited editing channel {channel.id}, retry after {e.retry_after}s")
            return False
        except Exception as e:
            shared.logger.debug(f"Failed to edit channel {channel.id}: {e}")
            return False

    async def batch_operations(self, operations):
        results = await asyncio.gather(*operations, return_exceptions=True)
        return results

    async def periodic_cleanup(self):
        """Periodic cleanup task - runs every 60 seconds"""
        await self.bot.wait_until_ready()
        cleanup_iteration = 0

        while not self.bot.is_closed():
            try:
                cleanup_iteration += 1
                shared.logger.debug(f"Periodic cleanup iteration #{cleanup_iteration}")

                # Core cleanup tasks
                await self.cleanup_orphaned_data()
                await self.cleanup_knock_tracking()

                now = time.time()

                # Clean up cooldowns and temporary tracking
                self.vc_creation_cooldowns = {uid: ts for uid, ts in self.vc_creation_cooldowns.items() if now - ts < 3600}
                self._hub_update_cooldowns = {k: ts for k, ts in self._hub_update_cooldowns.items() if now - ts < 60}
                self._name_update_debounce = {k: ts for k, ts in self._name_update_debounce.items() if now - ts < 10}
                self.creating_vcs.clear()

                # Clean up stale accepted knocks (shouldn't happen but safety check)
                for vc_id, users in list(self.accepted_knocks.items()):
                    if vc_id not in self.active_vcs:
                        # VC no longer exists, clean up all accepted knocks
                        for user_id in list(users.keys()):
                            await self.cleanup_accepted_knock(vc_id, user_id)
                        if vc_id in self.accepted_knocks:
                            del self.accepted_knocks[vc_id]

                # FALLBACK: Aggressive orphaned message cleanup (every iteration)
                for guild in self.bot.guilds:
                    try:
                        await self.cleanup_orphaned_hub_messages(guild)
                    except Exception as e:
                        shared.logger.error(f"Orphaned message cleanup error for guild {guild.id}: {e}")

                # FALLBACK: Force hub name reconciliation (every iteration)
                for guild in self.bot.guilds:
                    try:
                        await self.reconcile_hub_state(guild)
                    except Exception as e:
                        shared.logger.error(f"Hub reconciliation error for guild {guild.id}: {e}")

                # FALLBACK: Validate empty VCs are being cleaned up (every 3 iterations)
                if cleanup_iteration % 3 == 0:
                    try:
                        await self.validate_empty_vcs()
                    except Exception as e:
                        shared.logger.error(f"Empty VC validation error: {e}", exc_info=True)

                # Every 10 iterations (~10 minutes), do a more thorough health check
                if cleanup_iteration % 10 == 0:
                    shared.logger.info(f"Running thorough health check (iteration #{cleanup_iteration})")
                    try:
                        await self._health_check_all_vcs()
                    except Exception as e:
                        shared.logger.error(f"Health check error: {e}", exc_info=True)

                # SELF-HEALING: Every 5 iterations (~5 minutes), scan for orphaned VCs
                if cleanup_iteration % 5 == 0:
                    shared.logger.debug(f"Running self-heal scan (iteration #{cleanup_iteration})")
                    for guild in self.bot.guilds:
                        try:
                            await self.self_heal_scan(guild)
                        except Exception as e:
                            shared.logger.error(f"Self-heal scan error for guild {guild.id}: {e}", exc_info=True)

            except Exception as e:
                shared.logger.error(f"Periodic cleanup error (iteration #{cleanup_iteration}): {e}", exc_info=True)

            await asyncio.sleep(60)

    async def reconcile_hub_state(self, guild):
        """FALLBACK: Ensure hub state matches active VCs"""
        hub_id = await shared.get_config(f"hub_channel_id_{guild.id}")
        if not hub_id:
            return

        hub = guild.get_channel(int(hub_id))
        if not hub:
            return

        # Get active locked VCs (not ghost, not unlocked, not basic)
        active_locked = [v for v in self.active_vcs.values()
                        if v.get('guild_id') == guild.id
                        and not v.get('ghost', False)
                        and not v.get('unlocked', False)
                        and not v.get('is_basic', False)]  # FIX: exclude basic VCs

        # Determine expected hub name
        if not active_locked:
            expected_name = await shared.get_config(f"idle_name_{guild.id}", "🔑-join-locked-vcs")
        elif len(active_locked) == 1:
            owner = guild.get_member(active_locked[0]['owner_id'])
            if owner:
                clean = shared.sanitize_name(owner.display_name, owner.id)
                expected_name = f"🔑-join-{clean}-vc"
            else:
                expected_name = "🔑-join-locked-vcs"
        else:
            expected_name = "🔑-join-locked-vcs"

        # Update if needed
        if hub.name != expected_name:
            shared.logger.info(f"Reconciling hub name: '{hub.name}' -> '{expected_name}'")
            await self.update_hub_name(guild, force=True)

    async def self_heal_scan(self, guild):
        """
        SELF-HEALING: Scan guild for VCs that exist but aren't tracked.
        Attempt to reconnect orphaned VCs to prevent data loss.
        """
        try:
            cat_id = await shared.get_config(f"category_id_{guild.id}")
            if not cat_id:
                return

            category = guild.get_channel(int(cat_id))
            if not category:
                return

            reconnected = 0
            for channel in category.voice_channels:
                # Skip trigger channels
                if channel.name in [shared.TRIGGER_NAME, shared.TRIGGER_NAME_BASIC]:
                    continue

                # Skip if already tracked
                if channel.id in self.active_vcs:
                    continue

                # Skip empty channels (they'll be cleaned up naturally by Discord or when someone joins)
                if len(channel.members) == 0:
                    continue

                # This VC exists with members but isn't tracked - reconnect it!
                shared.logger.warning(
                    f"SELF-HEAL: Found untracked VC {channel.id} ({channel.name}) "
                    f"with {len(channel.members)} members in guild {guild.id}"
                )

                if await self.reconnect_vc(channel):
                    reconnected += 1

            if reconnected > 0:
                shared.logger.info(f"SELF-HEAL: Reconnected {reconnected} orphaned VCs in guild {guild.id}")
                await self.update_hub_name(guild, force=True)

        except Exception as e:
            shared.logger.error(f"SELF-HEAL: Scan failed for guild {guild.id}: {e}", exc_info=True)

    async def cleanup_orphaned_hub_messages(self, guild):
        """FALLBACK: Aggressively clean up orphaned hub messages"""
        hub_id = await shared.get_config(f"hub_channel_id_{guild.id}")
        if not hub_id:
            return

        hub = guild.get_channel(int(hub_id))
        if not hub:
            return

        # Get all active VC IDs in this guild
        active_vc_ids = {
            vc_id for vc_id, data in self.active_vcs.items()
            if data.get('guild_id') == guild.id
            and not data.get('ghost', False)
            and not data.get('unlocked', False)
            and not data.get('is_basic', False)
        }

        # Track valid message IDs
        valid_message_ids = {
            data.get('message_id') for vc_id, data in self.active_vcs.items()
            if data.get('guild_id') == guild.id
            and data.get('message_id')
            and not data.get('ghost', False)
            and not data.get('unlocked', False)
            and not data.get('is_basic', False)
        }
        valid_message_ids.discard(None)

        try:
            # Scan hub channel for orphaned messages
            orphaned_count = 0
            async for msg in hub.history(limit=100):
                if msg.author.id != self.bot.user.id:
                    continue
                if not msg.components:
                    continue

                # Check if this message belongs to a non-existent or wrong VC
                for row in msg.components:
                    for child in row.children:
                        custom_id = getattr(child, 'custom_id', None)
                        if not custom_id or not custom_id.startswith("knock:"):
                            continue

                        try:
                            vc_id = int(custom_id.split(":")[1])
                        except (ValueError, IndexError):
                            # Invalid custom_id, delete it
                            try:
                                await msg.delete()
                                orphaned_count += 1
                                shared.logger.info(f"Deleted orphaned hub message (invalid custom_id): {msg.id}")
                            except Exception:
                                pass
                            break

                        # Check if this VC ID is active
                        if vc_id not in active_vc_ids:
                            # VC doesn't exist or shouldn't have a hub message
                            try:
                                await msg.delete()
                                orphaned_count += 1
                                shared.logger.info(f"Deleted orphaned hub message for non-existent VC {vc_id}: {msg.id}")
                            except Exception:
                                pass
                            break

                        # Check if this message ID is valid
                        if msg.id not in valid_message_ids:
                            # Duplicate or stale message
                            try:
                                await msg.delete()
                                orphaned_count += 1
                                shared.logger.info(f"Deleted duplicate hub message for VC {vc_id}: {msg.id}")
                            except Exception:
                                pass
                            break

            if orphaned_count > 0:
                shared.logger.info(f"Cleaned up {orphaned_count} orphaned hub messages in guild {guild.id}")

        except Exception as e:
            shared.logger.error(f"Error scanning for orphaned messages in guild {guild.id}: {e}")

    async def validate_empty_vcs(self):
        """FALLBACK: Ensure empty VCs are being cleaned up properly - uses API verification"""
        for vc_id, data in list(self.active_vcs.items()):
            guild_id = data.get('guild_id')
            guild = self.bot.get_guild(guild_id) if guild_id else None

            if not guild:
                shared.logger.warning(f"VALIDATE EMPTY: No guild for VC {vc_id}, skipping")
                continue

            # === VERIFY VIA API ===
            channel, member_count = await shared.verify_channel_exists(self.bot, guild, vc_id)

            # Can't verify - skip to be safe
            if channel == "FORBIDDEN" or channel == "ERROR":
                shared.logger.debug(f"VALIDATE EMPTY: Cannot verify VC {vc_id}, skipping")
                continue

            # VC confirmed deleted - cleanup
            if not channel:
                shared.logger.warning(f"VALIDATE EMPTY: VC {vc_id} confirmed deleted via API, forcing cleanup")
                await self.cleanup_vc_by_id(vc_id)
                continue

            # VC is empty and no cleanup task is running
            if member_count == 0:
                has_cleanup_task = (
                    vc_id in self.tasks and
                    'cleanup' in self.tasks[vc_id] and
                    not self.tasks[vc_id]['cleanup'].done()
                )

                if not has_cleanup_task:
                    shared.logger.warning(f"VALIDATE EMPTY: VC {vc_id} confirmed empty via API, scheduling cleanup")
                    self.schedule_cleanup_task(channel)

    async def cleanup_knock_tracking(self):
        current_vcs = set(self.active_vcs.keys())
        to_remove = [vc_id for vc_id in self.knock_tracking.keys() if vc_id not in current_vcs]
        for vc_id in to_remove: 
            self.knock_tracking.pop(vc_id, None)
            self.pending_knocks.pop(vc_id, None)
            self.last_knock_ping.pop(vc_id, None)

    async def cleanup_orphaned_data(self):
        """Clean up orphaned VCs - uses API verification before cleanup"""
        to_remove = []
        for vc_id, vc_data in list(self.active_vcs.items()):
            guild_id = vc_data.get('guild_id')
            guild = self.bot.get_guild(guild_id) if guild_id else None

            # === VERIFY VIA API ===
            if guild:
                channel, member_count = await shared.verify_channel_exists(self.bot, guild, vc_id)

                # Can't verify - skip this VC to be safe
                if channel == "FORBIDDEN" or channel == "ERROR":
                    shared.logger.debug(f"ORPHAN CHECK: Cannot verify VC {vc_id}, skipping")
                    continue

                # Channel confirmed deleted
                if not channel:
                    shared.logger.info(f"ORPHAN CHECK: VC {vc_id} confirmed deleted via API")
                    to_remove.append(vc_id)
                    continue

                # Channel exists - check owner
                owner = guild.get_member(vc_data['owner_id'])
                if not owner:
                    if member_count > 0:
                        # Transfer to first member
                        shared.logger.info(f"ORPHAN CHECK: Owner missing for VC {vc_id}, transferring to member")
                        await self.transfer_ownership(channel, channel.members[0])
                    else:
                        # Empty and no owner
                        shared.logger.info(f"ORPHAN CHECK: VC {vc_id} empty with no owner")
                        to_remove.append(vc_id)
            else:
                # No guild - use cache as fallback but log warning
                shared.logger.warning(f"ORPHAN CHECK: No guild for VC {vc_id}, using cache fallback")
                channel = self.bot.get_channel(vc_id)
                if not channel:
                    to_remove.append(vc_id)

        for vc_id in to_remove:
            await self.cleanup_vc_by_id(vc_id)

    async def validate_trigger_channel(self):
        shared.logger.info("Trigger validation task started")
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            await asyncio.sleep(300)
            for guild in self.bot.guilds:
                found = any(c.name == self.trigger_name and isinstance(c, discord.VoiceChannel) for c in guild.channels)
                if not found: 
                    shared.logger.warning(f"Trigger channel missing in guild: {guild.name}")

    def get_guild_bitrate_limit(self, guild):
        if guild.premium_tier == 3: return 384000
        elif guild.premium_tier == 2: return 256000
        elif guild.premium_tier == 1: return 128000
        else: return 96000

    def get_max_voice_limit(self, guild): 
        return 99

    async def update_knock_panel(self, vc_id):
        """Update the knock management panel with current state"""
        vc_data = self.active_vcs.get(vc_id)
        if not vc_data:
            return

        thread_id = vc_data.get('thread_id')
        knock_msg_id = vc_data.get('knock_mgmt_msg_id')
        if not thread_id or not knock_msg_id:
            return

        # Validate thread using helper
        thread = await shared.check_thread_valid(self.bot, thread_id)
        if not thread:
            # Thread is invalid/archived/missing - clear references
            shared.logger.warning(f"Thread {thread_id} invalid for VC {vc_id}, clearing references")
            vc_data['thread_id'] = None
            vc_data['knock_mgmt_msg_id'] = None
            await self.save_state()
            return

        # Validate VC still exists
        vc = await shared.validate_vc_channel(self.bot, vc_id)
        if not vc:
            shared.logger.warning(f"VC {vc_id} no longer exists during knock panel update")
            return

        guild = vc.guild
        if not guild:
            shared.logger.error(f"VC {vc_id} has no guild")
            return

        # Validate owner is still a member
        owner = await shared.validate_member(guild, vc_data['owner_id'])
        if not owner:
            shared.logger.warning(f"Owner {vc_data['owner_id']} not found for VC {vc_id}")
            return

        try:
            msg = thread.get_partial_message(knock_msg_id)
            embed = shared.create_knock_management_embed(owner, self.pending_knocks.get(vc_id, []), guild, vc_data)
            view = shared.KnockManagementView(self.bot, self, owner.id, vc_id)

            has_pending = len(self.pending_knocks.get(vc_id, [])) > 0
            view.accept_btn.disabled = not has_pending
            view.deny_btn.disabled = not has_pending

            await msg.edit(embed=embed, view=view)
            self.bot.add_view(view, message_id=knock_msg_id)
            shared.logger.debug(f"Updated knock panel for VC {vc_id}")
        except discord.NotFound:
            # Message deleted - try to recreate it
            shared.logger.warning(f"Knock management message {knock_msg_id} not found, recreating")
            vc_data['knock_mgmt_msg_id'] = None

            try:
                embed = shared.create_knock_management_embed(owner, self.pending_knocks.get(vc_id, []), guild, vc_data)
                view = shared.KnockManagementView(self.bot, self, owner.id, vc_id)
                msg = await thread.send(content=owner.mention, embed=embed, view=view)
                self.bot.add_view(view, message_id=msg.id)
                vc_data['knock_mgmt_msg_id'] = msg.id
                shared.logger.info(f"Recreated knock management message for VC {vc_id}")
            except Exception as e:
                shared.logger.error(f"Failed to recreate knock management message: {e}")

            await self.save_state()
        except discord.Forbidden:
            shared.logger.error(f"No permission to update knock panel for VC {vc_id}")
        except Exception as e:
            shared.logger.error(f"Failed to update knock panel for VC {vc_id}: {e}", exc_info=True)

    async def handle_knock_ping(self, vc_id):
        """Send a knock notification to the VC owner in their thread"""
        vc_data = self.active_vcs.get(vc_id)
        if not vc_data or vc_data.get('mute_knock_pings', False):
            return
        if not self.pending_knocks.get(vc_id):
            return

        now = time.time()
        last_ping = self.last_knock_ping.get(vc_id, 0)
        if now - last_ping < 120:
            return

        # Validate thread
        thread_id = vc_data.get('thread_id')
        thread = await shared.check_thread_valid(self.bot, thread_id)
        if not thread:
            # Thread invalid - clear references and skip ping
            shared.logger.warning(f"Cannot send knock ping, thread {thread_id} invalid for VC {vc_id}")
            vc_data['thread_id'] = None
            vc_data['knock_mgmt_msg_id'] = None
            await self.save_state()
            return

        # Validate VC
        vc = await shared.validate_vc_channel(self.bot, vc_id)
        if not vc:
            shared.logger.warning(f"Cannot send knock ping, VC {vc_id} no longer exists")
            return

        # Validate owner
        owner = await shared.validate_member(vc.guild, vc_data['owner_id'])
        if not owner:
            shared.logger.warning(f"Cannot send knock ping, owner {vc_data['owner_id']} not found for VC {vc_id}")
            return

        try:
            pending_count = len(self.pending_knocks[vc_id])
            await thread.send(f"🔔 {owner.mention} You have {pending_count} pending knock request{'s' if pending_count > 1 else ''}!")
            self.last_knock_ping[vc_id] = now
            shared.logger.info(f"Sent knock ping for VC {vc_id} ({pending_count} requests)")
        except discord.Forbidden:
            shared.logger.error(f"No permission to send knock ping in thread {thread_id}")
        except Exception as e:
            shared.logger.error(f"Failed to send knock ping for VC {vc_id}: {e}")

    async def handle_knock_accepted(self, vc_id, user_id, thread):
        """Handle a knock being accepted - notify user in thread"""
        # Validate VC
        vc = await shared.validate_vc_channel(self.bot, vc_id)
        if not vc:
            shared.logger.warning(f"Cannot handle knock accepted, VC {vc_id} no longer exists")
            return

        try:
            # Validate user is still a member
            user = await shared.validate_member(thread.guild, user_id)
            if not user:
                shared.logger.warning(f"Cannot notify user {user_id}, not in guild")
                return

            # Try to add user to thread
            try:
                await thread.add_user(user)
            except discord.Forbidden:
                shared.logger.debug(f"No permission to add user {user_id} to thread {thread.id}")
            except Exception as e:
                shared.logger.debug(f"Failed to add user {user_id} to thread: {e}")

            # Send acceptance notification
            msg = await thread.send(f"✅ {user.mention} Your knock was accepted! You can now join **{vc.name}**.")

            if vc_id not in self.accepted_knocks:
                self.accepted_knocks[vc_id] = {}

            async def cleanup_after_timeout():
                await asyncio.sleep(300)
                await self.cleanup_accepted_knock(vc_id, user_id)

            task = asyncio.create_task(cleanup_after_timeout())
            self.accepted_knocks[vc_id][user_id] = {
                'msg_id': msg.id,
                'thread_id': thread.id,
                'task': task
            }
            shared.logger.info(f"Sent knock acceptance notification to user {user_id} for VC {vc_id}")
        except discord.Forbidden:
            shared.logger.error(f"No permission to send knock acceptance message in thread {thread.id}")
        except Exception as e:
            shared.logger.error(f"Failed to handle knock accepted for VC {vc_id}, user {user_id}: {e}")

    async def cleanup_accepted_knock(self, vc_id, user_id):
        if vc_id not in self.accepted_knocks or user_id not in self.accepted_knocks[vc_id]: 
            return
        
        data = self.accepted_knocks[vc_id].pop(user_id, None)
        if not data: 
            return
        
        if data.get('task') and not data['task'].done():
            data['task'].cancel()
        
        thread = self.bot.get_channel(data['thread_id'])
        if thread:
            try:
                msg = thread.get_partial_message(data['msg_id'])
                await msg.delete()
            except Exception: 
                pass
            
            try:
                user = thread.guild.get_member(user_id)
                if user: 
                    await thread.remove_user(user)
            except Exception: 
                pass
        
        if vc_id in self.accepted_knocks and not self.accepted_knocks[vc_id]:
            del self.accepted_knocks[vc_id]

    # --- COMMANDS ---

    @app_commands.command(name="vc_panel", description="[Admin] Configure locked VC system")
    @app_commands.checks.has_permissions(administrator=True)
    async def vc_panel_slash(self, interaction: discord.Interaction):
        embed = discord.Embed(title="🎤 Locked VC Configuration", description="Use the buttons below to configure the system.", color=discord.Color.blue())
        await interaction.response.send_message(embed=embed, view=shared.AdminPanelView(self.bot), ephemeral=True)

    # --- EVENTS ---

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        for vc_id, pending in list(self.pending_knocks.items()):
            if member.id in pending:
                pending.remove(member.id)
                await self.update_knock_panel(vc_id)

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.webhook_id or not message.author or not message.guild: 
            return
        if message.author.bot: 
            return

        hub_id = await shared.get_config(f"hub_channel_id_{message.guild.id}")
        if hub_id and message.channel.id == int(hub_id):
            try: 
                await message.delete()
            except Exception: 
                pass
            return

        if isinstance(message.channel, discord.Thread):
            # FIX: Check if thread is archived first
            if message.channel.archived:
                shared.logger.debug(f"Ignoring message in archived thread {message.channel.id}")
                return
            
            vc_id = None
            for vid, data in self.active_vcs.items():
                if data.get('thread_id') == message.channel.id:
                    vc_id = vid
                    break
            
            if not vc_id: 
                return
            vc_data = self.active_vcs.get(vc_id)
            if not vc_data or message.author.id != vc_data['owner_id']: 
                return
            if not message.mentions: 
                return

            vc = self.bot.get_channel(vc_id)
            if not vc: 
                return
            
            added = []
            failed = []
            already_vip = []
            
            for user in message.mentions:
                if user.bot: 
                    continue
                
                # FIX: Ensure user is a Member, not just a User
                member = message.guild.get_member(user.id)
                if not member:
                    failed.append(f"{user.mention} (not in server)")
                    continue
                
                if member.id in vc_data.get('bans', []): 
                    failed.append(f"{member.mention} (banned)")
                    continue
                
                # FIX: Check if already has VIP access to avoid redundant operations
                current_perms = vc.overwrites_for(member)
                if current_perms.connect is True:
                    already_vip.append(member.mention)
                    continue
                
                try:
                    if await self.safe_set_permissions(vc, member, connect=True, speak=True):
                        added.append(member.mention)
                    else:
                        failed.append(f"{member.mention} (permission error)")
                except Exception as e: 
                    shared.logger.error(f"Failed to add VIP {member.id}: {e}")
                    failed.append(f"{member.mention} (error)")
            
            if added or failed or already_vip:
                response = ""
                if added: 
                    response += f"✅ **VIP Access:** {', '.join(added)}\n"
                if already_vip:
                    response += f"ℹ️ **Already VIP:** {', '.join(already_vip)}\n"
                if failed: 
                    response += f"❌ **Failed:** {', '.join(failed)}"
                try: 
                    await message.reply(response.strip(), mention_author=False)
                except Exception as e:
                    shared.logger.error(f"Failed to reply to VIP message: {e}")

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before, after):
        if isinstance(after, discord.VoiceChannel):
            if after.id not in self.active_vcs or before.name == after.name: 
                return
            
            debounce_key = f"name_update_{after.id}"
            now = time.time()
            if self._name_update_debounce.get(debounce_key, 0) > now - 2: 
                return
            
            vc_data = self.active_vcs[after.id]
            is_locked = not vc_data.get('unlocked', False)
            prefix = "🔒 "
            try:
                if is_locked and not after.name.startswith(prefix): 
                    self._name_update_debounce[debounce_key] = now
                    await self.safe_edit_channel(after, name=f"{prefix}{after.name}")
                elif not is_locked and after.name.startswith(prefix): 
                    self._name_update_debounce[debounce_key] = now
                    await self.safe_edit_channel(after, name=after.name.replace(prefix, "", 1))
            except Exception: 
                pass
            await self.update_hub_embed(after.id)
        elif isinstance(after, discord.TextChannel) and before.id in self.vc_text_channels:
            if before.category_id != after.category_id:
                del self.vc_text_channels[before.id]

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel):
        if channel.id in self.active_vcs: 
            shared.logger.info(f"VC {channel.id} manual delete detected")
            await self.cleanup_vc(channel, manual_delete=True)
        if channel.id in self.vc_text_channels: 
            del self.vc_text_channels[channel.id]

    @commands.Cog.listener()
    async def on_voice_state_update(self, member, before, after):
        try:
            # FIX: Check for BOTH trigger channels (locked and basic VCs)
            if after.channel and after.channel.name in [self.trigger_name, shared.TRIGGER_NAME_BASIC]:
                if member.id in self.creating_vcs:
                    return
                self.creating_vcs.add(member.id)

                now = time.time()
                if now - self.vc_creation_cooldowns.get(member.id, 0) < 30:
                    self.creating_vcs.discard(member.id)
                    try:
                        await member.send("⏱️ Please wait 30s before creating another VC.")
                        await member.move_to(None)
                    except Exception:
                        pass
                    return

                # Determine VC type based on trigger channel
                is_basic = (after.channel.name == shared.TRIGGER_NAME_BASIC)
                shared.logger.info(f"User {member.id} triggered {'basic' if is_basic else 'locked'} VC creation")
                self.bot.loop.create_task(self._handle_vc_creation(member, after.channel.guild, is_basic=is_basic))
                return

            if before.channel and before.channel.id in self.active_vcs:
                vc_id = before.channel.id
                remaining = [m for m in before.channel.members if m.id != member.id]
                if len(remaining) == 0: 
                    self.schedule_cleanup_task(before.channel)
                elif member.id == self.active_vcs[vc_id]['owner_id']:
                    if vc_id in self.tasks:
                        if self.tasks[vc_id].get('cleanup'): 
                            self.tasks[vc_id]['cleanup'].cancel()
                            del self.tasks[vc_id]['cleanup']
                        if self.tasks[vc_id].get('transfer') and not self.tasks[vc_id]['transfer'].done(): 
                            return
                    if vc_id not in self.tasks: 
                        self.tasks[vc_id] = {}
                    self.tasks[vc_id]['transfer'] = asyncio.create_task(self.transfer_or_cleanup(before.channel, member.id))

            if after.channel and after.channel.id in self.active_vcs:
                vc_id = after.channel.id
                vc_data = self.active_vcs[vc_id]
                
                if vc_id in self.accepted_knocks and member.id in self.accepted_knocks[vc_id]:
                    asyncio.create_task(self.cleanup_accepted_knock(vc_id, member.id))
                
                # FIX: If rejoining owner, cancel transfer
                if member.id == vc_data['owner_id']:
                    ov = after.channel.overwrites_for(member)
                    if not ov.manage_channels:
                        await self.safe_set_permissions(after.channel, member, connect=True, move_members=True, manage_channels=True)
                    
                    # Cancel transfer task if owner rejoined
                    if vc_id in self.tasks and self.tasks[vc_id].get('transfer'):
                        self.tasks[vc_id]['transfer'].cancel()
                        del self.tasks[vc_id]['transfer']
                        shared.logger.info(f"Cancelled transfer for VC {vc_id} - owner rejoined")
                
                if vc_id in self.tasks:
                    if self.tasks[vc_id].get('cleanup'): 
                        self.tasks[vc_id]['cleanup'].cancel()
                        del self.tasks[vc_id]['cleanup']
                await self.update_hub_embed(vc_id)
        except Exception as e: 
            shared.logger.error(f"Voice state update error: {e}", exc_info=True)

    async def _handle_vc_creation(self, member, guild, is_basic=False):
        self.vc_creation_cooldowns[member.id] = time.time()
        try:
            for vdata in self.active_vcs.values():
                if vdata['owner_id'] == member.id:
                    try:
                        await member.move_to(None)
                        await member.send("❌ You already have a VC.")
                    except Exception:
                        pass
                    return
            await self.create_vc(member, guild, is_basic=is_basic)
        except Exception as e:
            shared.logger.error(f"Creation error: {e}")
        finally:
            self.creating_vcs.discard(member.id)

    async def _monitor_empty_vc_with_context(self, vc_id, guild_id):
        """Monitor empty VC with preserved guild context - uses API verification"""
        try:
            # Wait 60 seconds before checking
            await asyncio.sleep(60)

            # FIX: Check if cleanup already in progress
            if vc_id in self._cleanup_in_progress:
                shared.logger.debug(f"Cleanup already in progress for VC {vc_id}, aborting monitor")
                return

            # Check if VC still exists in our tracking
            if vc_id not in self.active_vcs:
                shared.logger.debug(f"VC {vc_id} no longer in active_vcs, monitor exiting")
                return

            # === VERIFY VIA API - NOT CACHE ===
            guild = self.bot.get_guild(guild_id) if guild_id else None
            if not guild:
                shared.logger.warning(f"MONITOR: Guild {guild_id} not found for VC {vc_id}, cannot verify - aborting")
                return  # Don't cleanup if we can't verify

            channel, member_count = await shared.verify_channel_exists(self.bot, guild, vc_id)

            # If we can't verify (API error or forbidden), don't cleanup
            if channel == "FORBIDDEN" or channel == "ERROR":
                shared.logger.warning(f"MONITOR: Cannot verify VC {vc_id}, skipping cleanup to be safe")
                return

            # Channel confirmed deleted - cleanup data only
            if not channel:
                shared.logger.info(f"MONITOR: VC {vc_id} confirmed deleted via API, cleaning up data")
                await self.cleanup_vc_by_id(vc_id)
            # Channel exists and is empty - cleanup
            elif member_count == 0:
                shared.logger.info(f"MONITOR: VC {vc_id} confirmed empty via API, initiating cleanup")
                await self.cleanup_vc(channel)
            # Channel has members - cancel cleanup
            else:
                shared.logger.info(f"MONITOR: VC {vc_id} has {member_count} members (verified via API), cleanup cancelled")

        except asyncio.CancelledError:
            shared.logger.debug(f"Monitor for VC {vc_id} was cancelled")
            raise  # Re-raise to properly handle cancellation
        except Exception as e:
            shared.logger.error(f"Empty VC monitor error for {vc_id}: {e}", exc_info=True)
            # FALLBACK: If monitor crashes, verify via API before any cleanup
            try:
                if vc_id in self.active_vcs:
                    guild = self.bot.get_guild(guild_id) if guild_id else None
                    if guild:
                        channel, member_count = await shared.verify_channel_exists(self.bot, guild, vc_id)
                        # Only cleanup if we can confirm it's deleted or empty
                        if channel == "FORBIDDEN" or channel == "ERROR":
                            shared.logger.warning(f"FALLBACK: Cannot verify VC {vc_id}, not cleaning up")
                        elif not channel:
                            shared.logger.warning(f"FALLBACK: VC {vc_id} confirmed deleted, cleaning up")
                            await self.cleanup_vc_by_id(vc_id)
                        elif member_count == 0:
                            shared.logger.warning(f"FALLBACK: VC {vc_id} confirmed empty, cleaning up")
                            await self.cleanup_vc(channel)
                        else:
                            shared.logger.info(f"FALLBACK: VC {vc_id} has {member_count} members, not cleaning up")
            except Exception as fallback_error:
                shared.logger.error(f"Fallback cleanup also failed for VC {vc_id}: {fallback_error}")

    async def monitor_empty_vc(self, vc_id):
        """Legacy method - redirects to new context-aware method"""
        vc_data = self.active_vcs.get(vc_id)
        guild_id = vc_data.get('guild_id') if vc_data else None
        await self._monitor_empty_vc_with_context(vc_id, guild_id)

    async def transfer_or_cleanup(self, voice_channel, old_owner_id):
        """Transfer ownership or cleanup if no eligible members"""
        max_attempts = 30
        attempts = 0
        
        while attempts < max_attempts:
            await asyncio.sleep(1)
            attempts += 1
            
            fresh_channel = self.bot.get_channel(voice_channel.id)
            if not fresh_channel: 
                return
            
            # FIX: Check if old owner rejoined
            vc_data = self.active_vcs.get(voice_channel.id)
            if vc_data:
                old_owner = fresh_channel.guild.get_member(old_owner_id)
                if old_owner and old_owner in fresh_channel.members:
                    shared.logger.info(f"Owner {old_owner_id} rejoined VC {voice_channel.id}, aborting transfer")
                    return
            
            members = [m for m in fresh_channel.members if not m.bot and m.id != old_owner_id]
            if not members: 
                self.schedule_cleanup_task(voice_channel)
                return
            
            new_owner = members[0]
            try: 
                await self.transfer_ownership(voice_channel, new_owner)
                return
            except Exception as e:
                shared.logger.debug(f"Transfer attempt {attempts} failed: {e}")
                continue
        
        # Max attempts reached
        shared.logger.warning(f"Transfer failed after {max_attempts} attempts for VC {voice_channel.id}")

    async def transfer_ownership(self, voice_channel, new_owner):
        vc_id = voice_channel.id
        vc_data = self.active_vcs.get(vc_id)
        if not vc_data: 
            return
        
        old_owner_id = vc_data['owner_id']
        old_owner = voice_channel.guild.get_member(old_owner_id)
        
        # Update owner
        vc_data['owner_id'] = new_owner.id
        await self.save_state()
        
        # Update permissions
        if old_owner:
            await self.safe_set_permissions(voice_channel, old_owner, overwrite=None)
        await self.safe_set_permissions(voice_channel, new_owner, connect=True, move_members=True, manage_channels=True)
        
        # Update channel name
        prefix = "🔒 " if not vc_data.get('unlocked', False) else ""
        clean_name = shared.sanitize_name(new_owner.display_name, new_owner.id)[:20]
        await self.safe_edit_channel(voice_channel, name=f"{prefix}{clean_name}'s VC")
        
        thread_id = vc_data.get('thread_id')
        if thread_id:
            thread = self.bot.get_channel(thread_id)
            if thread:
                # FIX: If thread is archived, delete it instead of unarchiving
                if isinstance(thread, discord.Thread) and thread.archived:
                    await self._delete_thread(thread_id)
                    vc_data['thread_id'] = None
                    vc_data['knock_mgmt_msg_id'] = None
                else:
                    try:
                        if old_owner:
                            try: 
                                await thread.remove_user(old_owner)
                            except Exception: 
                                pass
                        await thread.add_user(new_owner)
                        try: 
                            await thread.edit(name=f"🔒 {clean_name}'s VC Settings")
                        except Exception: 
                            pass
                        
                        embed = shared.create_knock_management_embed(new_owner, self.pending_knocks.get(vc_id, []), voice_channel.guild, vc_data)
                        view = shared.KnockManagementView(self.bot, self, new_owner.id, vc_id)
                        knock_msg = await thread.send(content=new_owner.mention, embed=embed, view=view)
                        self.bot.add_view(view, message_id=knock_msg.id)
                        vc_data['knock_mgmt_msg_id'] = knock_msg.id
                        await self.save_state()
                    except Exception as e:
                        shared.logger.error(f"Thread transfer failed: {e}")

        await self.delete_hub_message(vc_id)
        if not vc_data.get('ghost', False) and not vc_data.get('unlocked', False): 
            await self.create_hub_message(voice_channel)
        
        # FIX: Always update hub name after ownership transfer
        await self.update_hub_name(voice_channel.guild, force=True)

    async def update_hub_name(self, guild, force=False):
        hub_id = await shared.get_config(f"hub_channel_id_{guild.id}")
        if not hub_id:
            return

        async with self._hub_name_lock:
            now = time.time()
            self._hub_name_edits = [t for t in self._hub_name_edits if now - t < 600]

            hub_channel = guild.get_channel(int(hub_id))
            if not hub_channel:
                return

            # Get active locked VCs (not ghost, not unlocked, not basic)
            active = [v for v in self.active_vcs.values()
                     if v.get('guild_id') == guild.id
                     and not v.get('ghost', False)
                     and not v.get('unlocked', False)
                     and not v.get('is_basic', False)]  # FIX: exclude basic VCs

            shared.logger.debug(f"Hub name update: Found {len(active)} active locked VCs in guild {guild.id}")

            if not active:
                new_name = await shared.get_config(f"idle_name_{guild.id}", "🔑-join-locked-vcs")
                shared.logger.debug(f"No active VCs, using idle name: {new_name}")
            elif len(active) == 1:
                owner = guild.get_member(active[0]['owner_id'])
                if owner:
                    clean = shared.sanitize_name(owner.display_name, owner.id)
                    new_name = f"🔑-join-{clean}-vc"
                    shared.logger.debug(f"Single VC active, owner: {owner.display_name}, new name: {new_name}")
                else:
                    new_name = "🔑-join-locked-vcs"
                    shared.logger.warning(f"Single VC active but owner {active[0]['owner_id']} not found in guild")
            else:
                new_name = "🔑-join-locked-vcs"
                shared.logger.debug(f"Multiple VCs active ({len(active)}), using default name")

            if hub_channel.name == new_name:
                self._hub_rename_queue.pop(guild.id, None)
                await self._persist_hub_rename_queue()
                return

            # FIX: Check force flag BEFORE rate limit
            if force:
                # Force update bypasses rate limit IF possible
                if len(self._hub_name_edits) < 2:
                    self._last_hub_update = now
                    if await self.safe_edit_channel(hub_channel, name=new_name):
                        self._hub_name_edits.append(now)
                        self._hub_rename_queue.pop(guild.id, None)
                        await self._persist_hub_rename_queue()
                        shared.logger.info(f"FORCED hub rename to: {new_name}")
                        return
                    # Fall through to queue if edit failed

            # Rate limit check for non-forced updates
            if len(self._hub_name_edits) >= 2:
                self._hub_rename_queue[guild.id] = new_name
                await self._persist_hub_rename_queue()
                if not self._hub_rename_task or self._hub_rename_task.done():
                    self._hub_rename_task = asyncio.create_task(self._process_hub_rename_queue())
                shared.logger.debug(f"Hub rename queued due to rate limit: {new_name}")
                return

            if not force and now - self._last_hub_update < 5:
                self._hub_rename_queue[guild.id] = new_name
                await self._persist_hub_rename_queue()
                if not self._hub_rename_task or self._hub_rename_task.done():
                    self._hub_rename_task = asyncio.create_task(self._process_hub_rename_queue())
                return

            self._last_hub_update = now
            if await self.safe_edit_channel(hub_channel, name=new_name):
                self._hub_name_edits.append(now)
                self._hub_rename_queue.pop(guild.id, None)
                await self._persist_hub_rename_queue()
                shared.logger.info(f"Hub renamed to: {new_name}")
            else:
                self._hub_rename_queue[guild.id] = new_name
                await self._persist_hub_rename_queue()
                if not self._hub_rename_task or self._hub_rename_task.done():
                    self._hub_rename_task = asyncio.create_task(self._process_hub_rename_queue())

    async def _process_hub_rename_queue(self):
        """Process queued hub renames with exponential backoff and error recovery"""
        retry_delay = 30  # Start with 30 seconds (faster)
        max_delay = 300   # Max 5 minutes (reduced from 10)
        consecutive_failures = 0
        crash_count = 0
        max_crashes = 5

        try:
            while self._hub_rename_queue:
                await asyncio.sleep(retry_delay)

                async with self._hub_name_lock:
                    now = time.time()
                    self._hub_name_edits = [t for t in self._hub_name_edits if now - t < 600]

                    # FIX: Process multiple guilds per iteration (up to 2)
                    processed_count = 0
                    max_per_batch = 2

                    for guild_id, desired_name in list(self._hub_rename_queue.items()):
                        if processed_count >= max_per_batch:
                            break

                        # Check if rate limited globally
                        if len(self._hub_name_edits) >= 2:
                            shared.logger.debug("Hub rename rate limited, waiting...")
                            break  # Rate limited, wait for next iteration

                        # Validate guild still exists
                        guild = self.bot.get_guild(guild_id)
                        if not guild:
                            shared.logger.warning(f"Guild {guild_id} no longer exists, removing from queue")
                            self._hub_rename_queue.pop(guild_id, None)
                            continue

                        # Validate hub channel config
                        hub_id = await shared.get_config(f"hub_channel_id_{guild_id}")
                        if not hub_id:
                            shared.logger.warning(f"No hub channel configured for guild {guild_id}, removing from queue")
                            self._hub_rename_queue.pop(guild_id, None)
                            continue

                        # Validate hub channel exists
                        hub_channel = guild.get_channel(int(hub_id))
                        if not hub_channel:
                            shared.logger.warning(f"Hub channel {hub_id} not found in guild {guild_id}, removing from queue")
                            self._hub_rename_queue.pop(guild_id, None)
                            continue

                        # Ensure desired_name is a string
                        if not isinstance(desired_name, str):
                            shared.logger.warning(f"Invalid desired_name type for guild {guild_id}: {type(desired_name)}")
                            desired_name = str(desired_name)

                        # Skip if already correct
                        if hub_channel.name == desired_name:
                            shared.logger.debug(f"Hub channel {hub_id} already has correct name: {desired_name}")
                            self._hub_rename_queue.pop(guild_id, None)
                            continue

                        # Attempt rename
                        self._last_hub_update = now
                        try:
                            if await self.safe_edit_channel(hub_channel, name=desired_name):
                                self._hub_name_edits.append(now)
                                self._hub_rename_queue.pop(guild_id, None)
                                shared.logger.info(f"Processed queued hub rename to: {desired_name}")
                                processed_count += 1
                                consecutive_failures = 0
                                retry_delay = 30  # Reset delay on success
                            else:
                                shared.logger.warning(f"Failed to rename hub channel {hub_id}")
                                consecutive_failures += 1
                                retry_delay = min(retry_delay * 1.5, max_delay)
                                break  # Stop processing on failure
                        except Exception as e:
                            shared.logger.error(f"Error renaming hub channel {hub_id}: {e}")
                            consecutive_failures += 1
                            retry_delay = min(retry_delay * 1.5, max_delay)
                            break

                    # Persist queue after processing
                    await self._persist_hub_rename_queue()

                    # If no items processed due to rate limit, increase delay
                    if processed_count == 0 and self._hub_rename_queue:
                        consecutive_failures += 1
                        retry_delay = min(retry_delay * 1.5, max_delay)
                        shared.logger.debug(f"Hub rename queue stalled, retry in {retry_delay}s")

        except asyncio.CancelledError:
            shared.logger.info("Hub rename queue processor cancelled")
            raise  # Allow cancellation
        except Exception as e:
            crash_count += 1
            shared.logger.error(f"Hub rename queue processor crashed (crash #{crash_count}): {e}", exc_info=True)

            # Respawn after delay if queue still has items and not crashed too many times
            if self._hub_rename_queue and crash_count < max_crashes:
                backoff = min(60 * (2 ** crash_count), 600)  # Exponential backoff: 60s, 120s, 240s, etc.
                shared.logger.info(f"Respawning hub rename queue processor in {backoff}s (crash #{crash_count}/{max_crashes})")
                await asyncio.sleep(backoff)

                if self._hub_rename_queue:  # Check again after sleep
                    self._hub_rename_task = asyncio.create_task(self._process_hub_rename_queue())
            else:
                if crash_count >= max_crashes:
                    shared.logger.error(f"Hub rename queue processor crashed {max_crashes} times, giving up")
                else:
                    shared.logger.info("Hub rename queue is empty, not respawning processor")

    async def create_hub_message(self, voice_channel):
        vc_id = voice_channel.id

        if vc_id not in self._hub_message_locks:
            self._hub_message_locks[vc_id] = asyncio.Lock()

        async with self._hub_message_locks[vc_id]:
            vc_data = self.active_vcs.get(vc_id)
            # Don't create hub messages for unlocked, ghost, or basic VCs
            if not vc_data or vc_data.get('unlocked', False) or vc_data.get('ghost', False) or vc_data.get('is_basic', False):
                return False

            hub_id = await shared.get_config(f"hub_channel_id_{voice_channel.guild.id}")
            if not hub_id:
                return False
            hub = voice_channel.guild.get_channel(int(hub_id))
            if not hub:
                return False

            # FIX: Check message_id AFTER acquiring lock to prevent race
            if vc_data.get('message_id'):
                try:
                    # Verify existing message before creating new one
                    await hub.fetch_message(vc_data['message_id'])
                    shared.logger.debug(f"Hub message {vc_data['message_id']} already exists for VC {vc_id}")
                    return True  # Message exists, don't create duplicate
                except discord.NotFound:
                    # Message was deleted, clear invalid ID
                    vc_data['message_id'] = None

            # FIX: Check for existing duplicates and delete them (scan more history)
            try:
                messages_checked = 0
                max_messages = 200  # Check up to 200 messages
                async for m in hub.history(limit=max_messages):
                    if m.author.id != self.bot.user.id:
                        continue
                    if not m.components:
                        continue

                    for row in m.components:
                        for child in row.children:
                            if getattr(child, 'custom_id', None) == f"knock:{vc_id}":
                                try:
                                    await m.delete()
                                    shared.logger.info(f"Deleted duplicate knock message for VC {vc_id}")
                                except Exception:
                                    pass
            except Exception as e:
                shared.logger.error(f"Failed to check duplicates: {e}")
            
            owner = voice_channel.guild.get_member(vc_data['owner_id'])
            if not owner: 
                return False
            
            is_full = voice_channel.user_limit != 0 and len(voice_channel.members) >= voice_channel.user_limit
            embed = discord.Embed(color=discord.Color.red() if is_full else discord.Color.gold())
            embed.set_author(name=voice_channel.name, icon_url=owner.display_avatar.url)
            embed.description = "🔴 **FULL**\nClick **Knock** to request entry." if is_full else "Click **Knock** to request entry."
            
            view = shared.HubEntryView(self.bot, self, owner.id, vc_id)
            try:
                msg = await hub.send(embed=embed, view=view)
                self.bot.add_view(view, message_id=msg.id)
                vc_data['message_id'] = msg.id
                await self.save_state()
                shared.logger.info(f"Created hub message {msg.id} for VC {vc_id}")
                return True
            except Exception as e:
                shared.logger.error(f"Failed to create hub message: {e}")
                return False

    async def delete_hub_message(self, voice_id):
        vc_data = self.active_vcs.get(voice_id)
        if not vc_data or not vc_data.get('message_id'):
            return

        # FIX: Add lock protection to prevent race conditions
        if voice_id not in self._hub_message_locks:
            self._hub_message_locks[voice_id] = asyncio.Lock()

        async with self._hub_message_locks[voice_id]:
            # Double-check message_id still exists after acquiring lock
            if not vc_data.get('message_id'):
                return

            # FIX: Better guild resolution
            guild = None
            guild_id = vc_data.get('guild_id')
            if guild_id:
                guild = self.bot.get_guild(guild_id)

            if not guild:
                vc = self.bot.get_channel(voice_id)
                if vc and hasattr(vc, 'guild'):
                    guild = vc.guild

            if not guild and len(self.bot.guilds) == 1:
                guild = self.bot.guilds[0]

            if not guild:
                shared.logger.error(f"Cannot find guild for VC {voice_id}, orphaned message possible")
                return

            # CRITICAL: Clear message_id BEFORE attempting deletion (optimistic locking)
            msg_id = vc_data['message_id']
            vc_data['message_id'] = None
            await self.save_state()  # Persist immediately

            hub_id = await shared.get_config(f"hub_channel_id_{guild.id}")
            if hub_id:
                hub = guild.get_channel(int(hub_id))
                if hub:
                    try:
                        msg = hub.get_partial_message(msg_id)
                        await msg.delete()
                        shared.logger.info(f"Deleted hub message {msg_id} for VC {voice_id}")
                    except discord.NotFound:
                        # Already deleted - that's fine since we cleared message_id
                        shared.logger.debug(f"Hub message {msg_id} already deleted")
                    except Exception as e:
                        # Even if delete fails, message_id already cleared to prevent orphaning
                        shared.logger.error(f"Failed delete hub message: {e}")

    async def update_hub_embed(self, voice_id):
        cooldown_key = f"hub_update_{voice_id}"
        now = time.time()
        if now - self._hub_update_cooldowns.get(cooldown_key, 0) < 10: 
            return
        self._hub_update_cooldowns[cooldown_key] = now 
        
        vc_data = self.active_vcs.get(voice_id)
        if not vc_data or vc_data.get('ghost', False) or vc_data.get('unlocked', False): 
            return
            
        vc = self.bot.get_channel(voice_id)
        if not vc: 
            return
        
        # FIX: Validate guild exists
        if not vc.guild:
            shared.logger.error(f"VC {voice_id} has no guild")
            return
        
        if not vc_data.get('message_id'): 
            await self.create_hub_message(vc)
            return
            
        hub_id = await shared.get_config(f"hub_channel_id_{vc.guild.id}")
        hub = self.bot.get_channel(int(hub_id or 0))
        if not hub: 
            return
        
        try:
            msg = hub.get_partial_message(vc_data['message_id'])
            owner = vc.guild.get_member(vc_data['owner_id'])
            if not owner:
                return
            is_full = vc.user_limit != 0 and len(vc.members) >= vc.user_limit
            embed = discord.Embed(color=discord.Color.red() if is_full else discord.Color.gold())
            embed.set_author(name=vc.name, icon_url=owner.display_avatar.url)
            embed.description = "🔴 **FULL**\nClick **Knock** to request entry." if is_full else "Click **Knock** to request entry."
            await msg.edit(embed=embed)
        except discord.NotFound: 
            await self.create_hub_message(vc)
        except Exception: 
            pass

    async def get_or_create_hub(self, guild, category):
        hub_id = await shared.get_config(f"hub_channel_id_{guild.id}")
        if hub_id:
            hub = guild.get_channel(int(hub_id))
            if hub: 
                return hub
            await shared.set_config(f'hub_channel_id_{guild.id}', None)
        for channel in category.text_channels:
            if channel.name in ["join-locked-vcs", "🔑-join-locked-vcs"]: 
                await shared.set_config(f'hub_channel_id_{guild.id}', channel.id)
                return channel
        try:
            # FIX: Allow thread chatting
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=True, send_messages=False, send_messages_in_threads=True),
                guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_messages=True)
            }
            hub = await guild.create_text_channel("🔑-join-locked-vcs", category=category, overwrites=overwrites)
            await shared.set_config(f'hub_channel_id_{guild.id}', hub.id)
            return hub
        except Exception: 
            return None

    async def create_vc(self, owner, guild, is_basic=False):
        cat_id = await shared.get_config(f"category_id_{guild.id}")
        if not cat_id:
            return
        category = guild.get_channel(int(cat_id))
        if not category:
            return

        if not category.permissions_for(guild.me).manage_channels:
            shared.logger.error("Missing manage_channels permission in category")
            return

        if len([c for c in category.channels if isinstance(c, discord.VoiceChannel)]) >= 49:
            return

        # FIX: Basic VCs don't use presets
        clean_name = shared.sanitize_name(owner.display_name, owner.id)[:20]
        if is_basic:
            vc_name = f"{clean_name}'s VC"  # No lock emoji
            vc_limit, vc_bitrate, vc_bans = 0, 64000, []
        else:
            # Locked VCs: load presets
            user_presets = await shared.get_user_presets(owner.id)
            vc_name = f"🔒 {clean_name}'s VC"
            vc_limit, vc_bitrate, vc_bans = 0, 64000, []

            default_preset = next((v for k, v in user_presets.items() if k.lower() == "default"), None)
            if default_preset:
                vc_name = default_preset.get("name", vc_name)[:100]
                vc_limit = max(0, min(default_preset.get("limit", 0), self.get_max_voice_limit(guild)))
                vc_bitrate = min(default_preset.get("bitrate", 64000), self.get_guild_bitrate_limit(guild))
                vc_bans = [uid for uid in default_preset.get("bans", []) if guild.get_member(uid)]

        # FIX: Basic VCs allow everyone to connect
        if is_basic:
            overwrites = {
                owner: discord.PermissionOverwrite(connect=True, move_members=True, manage_channels=True),
                guild.me: discord.PermissionOverwrite(connect=True, manage_channels=True)
                # No default_role restriction for basic VCs
            }
        else:
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(connect=False),
                owner: discord.PermissionOverwrite(connect=True, move_members=True, manage_channels=True),
                guild.me: discord.PermissionOverwrite(connect=True, manage_channels=True)
            }
        
        try:
            vc = await guild.create_voice_channel(name=vc_name, category=category, overwrites=overwrites, user_limit=vc_limit, bitrate=vc_bitrate)

            # Apply bans for locked VCs only
            if not is_basic and vc_bans:
                ops = [self.safe_set_permissions(vc, guild.get_member(bid), connect=False) for bid in vc_bans if guild.get_member(bid)]
                if ops:
                    await self.batch_operations(ops)

            try:
                await owner.move_to(vc)
            except Exception:
                pass

            async with self._active_vcs_lock:
                self.active_vcs[vc.id] = {
                    'owner_id': owner.id,
                    'message_id': None,
                    'knock_mgmt_msg_id': None,
                    'thread_id': None,
                    'ghost': False,
                    'unlocked': is_basic,  # FIX: Basic VCs are marked as unlocked
                    'is_basic': is_basic,  # FIX: New flag to distinguish basic VCs
                    'bans': [] if is_basic else vc_bans,
                    'mute_knock_pings': False,
                    'guild_id': guild.id
                }
            await self.save_state()

            self.pending_knocks[vc.id] = []
            self.last_knock_ping[vc.id] = 0

            # FIX: Only create hub message and thread for LOCKED VCs
            if not is_basic:
                hub = await self.get_or_create_hub(guild, category)
                if hub:
                    await self.create_hub_message(vc)
                    perms = hub.permissions_for(guild.me)
                    try:
                        if not perms.create_private_threads or not perms.manage_threads:
                            thread = await hub.create_thread(name=f"🔒 {clean_name}'s VC Settings", auto_archive_duration=1440)
                        else:
                            thread = await hub.create_thread(name=f"🔒 {clean_name}'s VC Settings", type=discord.ChannelType.private_thread, auto_archive_duration=1440, invitable=False)

                        try:
                            await thread.add_user(owner)
                        except discord.Forbidden:
                            await thread.send(f"⚠️ {owner.mention} - Access VC settings here!")

                        view = shared.KnockManagementView(self.bot, self, owner.id, vc.id)
                        embed = shared.create_knock_management_embed(owner, [], guild, self.active_vcs[vc.id])
                        knock_msg = await thread.send(content=owner.mention, embed=embed, view=view)
                        self.bot.add_view(view, message_id=knock_msg.id)
                        self.active_vcs[vc.id].update({'knock_mgmt_msg_id': knock_msg.id, 'thread_id': thread.id})
                        await self.save_state()
                    except Exception as e:
                        # FIX: Log thread creation failure but don't fail entire VC creation
                        shared.logger.error(f"Failed to create thread for VC {vc.id}: {e}")
                        shared.logger.warning(f"VC {vc.id} created without settings thread")

                # Update hub name for locked VCs only
                await self.update_hub_name(guild, force=True)
        except Exception as e:
            shared.logger.error(f"Failed to create VC: {e}")

    async def cleanup_vc(self, voice_channel, manual_delete=False):
        """Clean up a VC - IMPROVED with better error handling"""
        vc_id = voice_channel.id
        guild = voice_channel.guild

        shared.logger.info(f"cleanup_vc called for VC {vc_id} (manual_delete={manual_delete})")

        # Delete the voice channel first if not manually deleted
        if not manual_delete:
            try:
                await voice_channel.delete()
                shared.logger.info(f"Deleted voice channel {vc_id}")
            except discord.NotFound:
                shared.logger.debug(f"Voice channel {vc_id} already deleted")
            except Exception as e:
                shared.logger.error(f"Error deleting voice channel {vc_id}: {e}")
                # Continue with cleanup even if deletion fails

        # Clean up all associated data (this will also handle hub name update)
        await self.cleanup_vc_by_id(vc_id)


async def setup(bot):
    await bot.add_cog(VC(bot))
