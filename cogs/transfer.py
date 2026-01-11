import discord
from discord.ext import commands
from discord import app_commands
from discord import ui
import aiohttp
import io
import asyncio
from typing import Optional, List

# --- 1. Modals (Pop-up Forms) ---

class TransferMediaModal(ui.Modal):
    """Modal for initiating a media transfer."""
    def __init__(self, cog: 'AdminPanelCog'):
        super().__init__(title="Transfer Media")
        self.cog = cog
        
        self.source_channel_id = ui.TextInput(
            label="Source Channel ID",
            placeholder="Right-click the channel and 'Copy Channel ID'",
            style=discord.TextStyle.short,
            required=True
        )
        self.destination_channel_id = ui.TextInput(
            label="Destination Channel ID",
            placeholder="Right-click the channel and 'Copy Channel ID'",
            style=discord.TextStyle.short,
            required=True
        )
        self.start_after_message_id = ui.TextInput(
            label="Start After Message ID (Optional)",
            placeholder="Leave blank to start from the beginning.",
            style=discord.TextStyle.short,
            required=False
        )
        
        self.add_item(self.source_channel_id)
        self.add_item(self.destination_channel_id)
        self.add_item(self.start_after_message_id)

    async def on_submit(self, interaction: discord.Interaction):
        # FIX: Defer immediately to prevent "Interaction Failed" timeout
        await interaction.response.defer(ephemeral=True)
        await self.cog._start_media_transfer(
            interaction,
            self.source_channel_id.value,
            self.destination_channel_id.value,
            self.start_after_message_id.value or None
        )

class TransferBansModal(ui.Modal):
    """Modal for initiating a ban transfer."""
    def __init__(self, cog: 'AdminPanelCog'):
        super().__init__(title="Transfer Bans")
        self.cog = cog

        self.source_server_id = ui.TextInput(
            label="Source Server ID",
            placeholder="Right-click the server icon and 'Copy Server ID'",
            style=discord.TextStyle.short,
            required=True
        )
        self.target_server_id = ui.TextInput(
            label="Target Server ID",
            placeholder="Right-click the server icon and 'Copy Server ID'",
            style=discord.TextStyle.short,
            required=True
        )
        self.add_item(self.source_server_id)
        self.add_item(self.target_server_id)

    async def on_submit(self, interaction: discord.Interaction):
        # FIX: Defer immediately to prevent "Interaction Failed" timeout
        await interaction.response.defer(ephemeral=True)
        await self.cog._start_ban_transfer(
            interaction,
            self.source_server_id.value,
            self.target_server_id.value
        )

# --- 2. The Persistent Panel View ---

class AdminTransferView(ui.View):
    """
    A persistent view for the admin panel.
    """
    def __init__(self):
        super().__init__(timeout=None)

    @ui.button(label="Transfer Media", style=discord.ButtonStyle.secondary, emoji="📸", custom_id="admin_panel:transfer_media_v1")
    async def transfer_media_button(self, interaction: discord.Interaction, button: ui.Button):
        cog = interaction.client.get_cog('AdminPanelCog')
        if not cog:
            await interaction.response.send_message("Error: The Admin Panel cog is not loaded.", ephemeral=True)
            return
        
        # Wrap in try/except to catch potential modal errors
        try:
            modal = TransferMediaModal(cog)
            await interaction.response.send_modal(modal)
        except Exception as e:
            print(f"[Error] Failed to send Media Modal: {e}")
            await interaction.response.send_message("Failed to open modal. Check console.", ephemeral=True)

    @ui.button(label="Transfer Bans", style=discord.ButtonStyle.danger, emoji="🔨", custom_id="admin_panel:transfer_bans_v1")
    async def transfer_bans_button(self, interaction: discord.Interaction, button: ui.Button):
        cog = interaction.client.get_cog('AdminPanelCog')
        if not cog:
            await interaction.response.send_message("Error: The Admin Panel cog is not loaded.", ephemeral=True)
            return
            
        try:
            modal = TransferBansModal(cog)
            await interaction.response.send_modal(modal)
        except Exception as e:
            print(f"[Error] Failed to send Bans Modal: {e}")
            await interaction.response.send_message("Failed to open modal. Check console.", ephemeral=True)

# --- 3. The Main Cog ---

class AdminPanelCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.session: Optional[aiohttp.ClientSession] = None
        print("AdminPanelCog initialized.")

    async def cog_load(self):
        self.session = aiohttp.ClientSession()
        print("AdminPanelCog loaded and session created.")

    async def cog_unload(self):
        if self.session:
            await self.session.close()
            print("AdminPanelCog unloaded and session closed.")

    @app_commands.command(name="transfer_panel", description="Access the admin transfer panel.")
    async def transfer_panel(self, interaction: discord.Interaction):
        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("❌ Administrator permission required.", ephemeral=True)
        embed = discord.Embed(
            title="Admin Transfer Panel",
            description="Select a transfer task to begin.\nEach button will open a form to ask for the required IDs.",
            color=discord.Color.blue()
        )
        await interaction.response.send_message(embed=embed, view=AdminTransferView(), ephemeral=True)

    # --- 3.2. Media Transfer Logic ---
    
    async def _start_media_transfer(self, interaction: discord.Interaction, 
                                  source_channel_id: str, 
                                  destination_channel_id: str,
                                  start_after_message_id: Optional[str] = None):
        
        # NOTE: interaction.response is already deferred in on_submit. Use followup.
        
        if not self.session or self.session.closed:
            print("[Media Transfer] Error: Session not ready.")
            await interaction.followup.send("The bot is not ready. Please wait a moment and try again.", ephemeral=True)
            return

        after_message_obj = None
        start_message = f"from the beginning of channel `{source_channel_id}`"
        if start_after_message_id:
            try:
                message_id_int = int(start_after_message_id)
                after_message_obj = discord.Object(id=message_id_int)
                start_message = f"starting **after** message ID `{start_after_message_id}`"
            except ValueError:
                await interaction.followup.send("Error: The 'Start After Message ID' was not a valid number.", ephemeral=True)
                return
        
        await interaction.followup.send(
            f"✅ **Media Transfer Started!**\n\n"
            f"I will start copying {start_message} to `{destination_channel_id}`.\n\n"
            "**IMPORTANT:** You requested a 15-minute cooldown. This will take a **very long time**.\n"
            "I will send you a DM when it's 100% complete.",
            ephemeral=True
        )

        try:
            source_channel_id_int = int(source_channel_id)
            destination_channel_id_int = int(destination_channel_id)
        except ValueError:
            await interaction.followup.send("Error: Channel IDs must be numbers.", ephemeral=True)
            return

        source_channel = self.bot.get_channel(source_channel_id_int)
        destination_channel = self.bot.get_channel(destination_channel_id_int)

        if not source_channel or not isinstance(source_channel, discord.TextChannel):
            await interaction.followup.send(f"Error: I cannot find the source text channel (`{source_channel_id}`).", ephemeral=True)
            return
            
        if not destination_channel or not isinstance(destination_channel, discord.TextChannel):
            await interaction.followup.send(f"Error: I cannot find the destination text channel (`{destination_channel_id}`).", ephemeral=True)
            return
        
        messages_processed = 0
        media_transferred = 0

        try:
            async for message in source_channel.history(limit=None, oldest_first=True, after=after_message_obj):
                messages_processed += 1
                
                if messages_processed % 20 == 0: 
                    print(f"[Media Transfer] Processed {messages_processed} messages from #{source_channel.name}...")

                if not message.attachments and not message.embeds and not message.content:
                    continue
                
                main_embed = discord.Embed(
                    description=message.content or None,
                    color=0x2B2D31
                )
                main_embed.set_author(
                    name=f"Originally posted by {message.author.display_name}",
                    icon_url=message.author.display_avatar.url
                )
                
                embeds_to_send: List[discord.Embed] = [main_embed]
                files_to_send: List[discord.File] = []
                links_to_add_to_desc: List[str] = []
                
                has_media = False
                has_non_media_files = False
                main_embed_has_image = False 

                for att in message.attachments:
                    if len(embeds_to_send) >= 10:
                        links_to_add_to_desc.append(f"Additional attachment: {att.filename} (Limit reached)")
                        continue

                    file_bytes = await self.download_file(att.url)
                    if not file_bytes:
                        links_to_add_to_desc.append(f"Failed to download: {att.filename}")
                        continue
                    
                    discord_file = discord.File(
                        io.BytesIO(file_bytes),
                        filename=att.filename,
                        description=att.description,
                        spoiler=att.is_spoiler()
                    )
                    files_to_send.append(discord_file)

                    if self.is_media_file(att.filename):
                        has_media = True
                        if self.is_image_file(att.filename):
                            if not main_embed_has_image:
                                main_embed.set_image(url=f"attachment://{att.filename}")
                                main_embed_has_image = True
                            else:
                                media_embed = discord.Embed(color=0x2B2D31)
                                media_embed.set_image(url=f"attachment://{att.filename}")
                                embeds_to_send.append(media_embed)
                    else:
                        links_to_add_to_desc.append(f"Attached file: [{att.filename}]({att.url})")
                        has_non_media_files = True
                
                if message.embeds:
                    for embed in message.embeds:
                        if embed.type in ['image', 'video', 'gifv'] and embed.url:
                            if len(embeds_to_send) >= 10:
                                links_to_add_to_desc.append(f"Additional media: {embed.url} (Limit reached)")
                                continue
                            
                            has_media = True
                            if not main_embed_has_image:
                                main_embed.set_image(url=embed.url)
                                main_embed_has_image = True
                            else:
                                media_embed = discord.Embed(color=0x2B2D31)
                                media_embed.set_image(url=embed.url)
                                embeds_to_send.append(media_embed)

                if links_to_add_to_desc:
                    current_desc = main_embed.description or ""
                    links_text = "\n\n**Additional Items:**\n" + "\n".join(links_to_add_to_desc)
                    main_embed.description = (current_desc + links_text).strip()

                if has_media or message.content or has_non_media_files:
                    try:
                        await destination_channel.send(
                            embeds=embeds_to_send,
                            files=files_to_send,
                            allowed_mentions=discord.AllowedMentions.none()
                        )
                        media_transferred += 1
                    
                    except discord.HTTPException as e:
                        print(f"[Media Transfer] Failed to send message {message.id}. Error: {e}")
                        fallback_embed = discord.Embed(
                            description=message.content or "This message had content that failed to transfer.",
                            color=0xED4245 
                        )
                        fallback_embed.set_author(
                            name=f"Originally posted by {message.author.display_name}",
                            icon_url=message.author.display_avatar.url
                        )
                        try:
                            await destination_channel.send(embed=fallback_embed, allowed_mentions=discord.AllowedMentions.none())
                            media_transferred += 1
                        except Exception:
                             pass
                        
                    finally:
                        for f in files_to_send:
                            f.close()
                    
                    await asyncio.sleep(900) # 15 minutes
        
        except discord.Forbidden:
            print(f"[Media Transfer] Transfer failed. I lost permissions.")
            return
        except Exception as e:
            print(f"[Media Transfer] An unexpected error occurred: {e}")
            return

        try:
            await interaction.user.send(
                f"✅ **Media Transfer Complete!**\n\n"
                f"Finished copying to `#{destination_channel.name}`.\n"
                f"Processed: `{messages_processed}` messages.\n"
                f"Transferred: `{media_transferred}` media posts."
            )
        except Exception:
            await destination_channel.send(f"✅ Media transfer complete.")


    # --- 3.3. Ban Transfer Logic ---

    async def _start_ban_transfer(self, interaction: discord.Interaction, 
                                source_server_id: str, 
                                target_server_id: str):
        
        # NOTE: interaction.response is already deferred. Use followup.
        
        await interaction.followup.send(
            f"✅ **Ban Transfer Started!**\n\n"
            f"I will copy all bans from server `{source_server_id}` to server `{target_server_id}`.\n\n"
            "This may take a few minutes. I will send you a DM when it's complete.",
            ephemeral=True
        )

        try:
            source_id_int = int(source_server_id)
            target_id_int = int(target_server_id)
        except ValueError:
            await interaction.followup.send("Error: Server IDs must be numbers.", ephemeral=True)
            return
        
        if source_id_int == target_id_int:
             await interaction.followup.send("Error: Source and Target servers cannot be the same.", ephemeral=True)
             return

        source_guild = self.bot.get_guild(source_id_int)
        target_guild = self.bot.get_guild(target_id_int)

        if not source_guild:
            await interaction.followup.send(f"Error: I am not in the source server (`{source_server_id}`).", ephemeral=True)
            return
        if not target_guild:
            await interaction.followup.send(f"Error: I am not in the target server (`{target_server_id}`).", ephemeral=True)
            return
        
        if not source_guild.me.guild_permissions.ban_members:
             await interaction.followup.send(f"Error: I need 'Ban Members' permission in the *source* server (`{source_guild.name}`) to see the ban list.", ephemeral=True)
             return
        if not target_guild.me.guild_permissions.ban_members:
             await interaction.followup.send(f"Error: I need 'Ban Members' permission in the *target* server (`{target_guild.name}`) to apply bans.", ephemeral=True)
             return

        transferred_count = 0
        skipped_count = 0
        failed_count = 0
        
        try:
            print("[Ban Transfer] Fetching target server ban list...")
            target_bans = {entry.user.id async for entry in target_guild.bans(limit=None)}
            
            print("[Ban Transfer] Fetching source server ban list...")
            source_ban_list = [entry async for entry in source_guild.bans(limit=None)]
            
            for ban_entry in source_ban_list:
                if ban_entry.user.id in target_bans:
                    skipped_count += 1
                else:
                    try:
                        reason = f"Ban transferred from {source_guild.name}. Original reason: {ban_entry.reason or 'N/A'}"
                        # Use discord.Object and delete_message_days=0 as requested
                        await target_guild.ban(
                            discord.Object(id=ban_entry.user.id),
                            reason=reason[:512],
                            delete_message_days=0
                        )
                        transferred_count += 1
                        print(f"[Ban Transfer] Banned {ban_entry.user.name} (ID: {ban_entry.user.id}).")
                    except discord.Forbidden:
                        failed_count += 1
                    except discord.HTTPException as e:
                        print(f"[Ban Transfer] FAILED to ban {ban_entry.user.name}. Error: {e}")
                        failed_count += 1
                
                await asyncio.sleep(1)

        except Exception as e:
            print(f"[Ban Transfer] An unexpected error occurred: {e}")
            await interaction.followup.send(f"An unexpected error occurred: {e}", ephemeral=True)
            return

        try:
            await interaction.user.send(
                f"✅ **Ban Transfer Complete!**\n\n"
                f"Finished copying bans from `{source_guild.name}` to `{target_guild.name}`.\n\n"
                f"• **Transferred:** `{transferred_count}`\n"
                f"• **Skipped (already banned):** `{skipped_count}`\n"
                f"• **Failed (check console):** `{failed_count}`"
            )
        except Exception:
            pass


    # --- 3.4. Helper Functions ---
    
    async def download_file(self, url: str) -> Optional[bytes]:
        if not self.session: return None
        try:
            async with self.session.get(url) as resp:
                if resp.status == 200: return await resp.read()
                return None
        except Exception: return None

    def is_image_file(self, filename: str) -> bool:
        return filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp'))

    def is_media_file(self, filename: str) -> bool:
        return filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp', '.mp4', '.mov', '.webm'))
    
    @transfer_panel.error
    async def transfer_panel_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message("You must be an Administrator to use this command.", ephemeral=True)
        else:
            if not interaction.response.is_done():
                await interaction.response.send_message(f"Error: {error}", ephemeral=True)

# --- 4. Setup Function ---

async def setup(bot: commands.Bot):
    """
    The setup function to load the cog.
    Removed the getattr check so reloading properly updates the Buttons/View.
    """
    bot.add_view(AdminTransferView())
    print("AdminTransferView added (refreshed).")
        
    await bot.add_cog(AdminPanelCog(bot))


