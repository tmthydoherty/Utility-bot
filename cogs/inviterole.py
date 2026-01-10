import discord
from discord import app_commands, ui
from discord.ext import commands
import asyncio
import json
import os

# ---------------------------------------------------------------------------
# !! REMINDER !!
#
# 1. BOT PERMISSIONS:
#    Your bot MUST have "Manage Server" and "Manage Roles" permissions.
#
# 2. BOT INTENTS:
#    Your main.py file MUST have `intents.invites = True` and
#    `intents.members = True` enabled.
#
# ---------------------------------------------------------------------------


# --- MODAL FOR ADDING/UPDATING A LINK ---
class AddLinkModal(ui.Modal, title="Add/Update Invite Link"):
    def __init__(self, view: 'InvitePanelView'):
        super().__init__(timeout=None)
        self.view = view # Store the parent view
        
        # Form fields
        self.invite_code = ui.TextInput(
            label="Invite Code",
            placeholder="e.g., aBcDeF1 or discord.gg/aBcDeF1",
            style=discord.TextStyle.short,
            required=True
        )
        self.role_input = ui.TextInput(
            label="Role (Name or ID)",
            placeholder="e.g., 'Team Red' or 123456789...",
            style=discord.TextStyle.short,
            required=True
        )
        self.add_item(self.invite_code)
        self.add_item(self.role_input)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        cog = self.view.cog
        guild_id_str = str(guild.id)
        
        # Clean up the invite code
        cleaned_code = self.invite_code.value.split('/')[-1]

        # Find the role
        role: discord.Role = None
        role_name_or_id = self.role_input.value
        try:
            # Try by ID first
            role = guild.get_role(int(role_name_or_id))
        except ValueError:
            # If not an ID, try by name
            role = discord.utils.get(guild.roles, name=role_name_or_id)
        
        if role is None:
            await interaction.response.send_message(f"❌ **Role Not Found!**\nCould not find a role matching `{role_name_or_id}`. Please check spelling and case.", ephemeral=True)
            return

        # Ensure the guild has a map
        if guild_id_str not in cog.invite_role_map:
            cog.invite_role_map[guild_id_str] = {}

        # Add/update the link
        cog.invite_role_map[guild_id_str][cleaned_code] = role.id
        cog.save_role_map()

        # Acknowledge the modal and refresh the original panel
        await interaction.response.defer(thinking=True, ephemeral=True)
        new_embed = await cog.generate_list_embed(guild)
        await self.view.message.edit(embed=new_embed, view=self.view)
        await interaction.followup.send(f"✅ **Link Created!**\nMembers who join using `{cleaned_code}` will now get the {role.mention} role.", ephemeral=True)
    
    async def on_error(self, interaction: discord.Interaction, error: Exception):
        print(f"Error in AddLinkModal: {error}")
        await interaction.response.send_message("An unexpected error occurred in the form.", ephemeral=True)


# --- MODAL FOR REMOVING A LINK ---
class RemoveLinkModal(ui.Modal, title="Remove Invite Link"):
    def __init__(self, view: 'InvitePanelView'):
        super().__init__(timeout=None)
        self.view = view # Store the parent view
        
        # Form field
        self.invite_code = ui.TextInput(
            label="Invite Code to Remove",
            placeholder="e.g., aBcDeF1 (must be an exact match)",
            style=discord.TextStyle.short,
            required=True
        )
        self.add_item(self.invite_code)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        cog = self.view.cog
        guild_id_str = str(guild.id)
        cleaned_code = self.invite_code.value.split('/')[-1]

        guild_map = cog.invite_role_map.get(guild_id_str, {})

        if cleaned_code in guild_map:
            # Remove the link
            role_id = guild_map.pop(cleaned_code)
            cog.save_role_map()
            
            role = guild.get_role(role_id)
            role_name = f"`@{role.name}`" if role else f"`Deleted Role (ID: {role_id})`"
            
            # Acknowledge the modal and refresh the original panel
            await interaction.response.defer(thinking=True, ephemeral=True)
            new_embed = await cog.generate_list_embed(guild)
            await self.view.message.edit(embed=new_embed, view=self.view)
            await interaction.followup.send(f"✅ **Link Removed!**\nInvite code `{cleaned_code}` will no longer assign the {role_name} role.", ephemeral=True)
        else:
            await interaction.response.send_message(f"❌ **Not Found!**\nThe invite code `{cleaned_code}` is not linked to any role.", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        print(f"Error in RemoveLinkModal: {error}")
        await interaction.response.send_message("An unexpected error occurred in the form.", ephemeral=True)


# --- THE INTERACTIVE PANEL VIEW ---
class InvitePanelView(ui.View):
    def __init__(self, cog: 'InviteTracker'):
        super().__init__(timeout=300) # 5 minute timeout
        self.cog = cog
        self.message: discord.InteractionMessage = None # Will store the message this view is attached to

    @ui.button(label="Add/Update Link", style=discord.ButtonStyle.green, emoji="🔗")
    async def add_link(self, interaction: discord.Interaction, button: ui.Button):
        """Pops up the modal to add a new link."""
        modal = AddLinkModal(self)
        await interaction.response.send_modal(modal)

    @ui.button(label="Remove Link", style=discord.ButtonStyle.red, emoji="🗑️")
    async def remove_link(self, interaction: discord.Interaction, button: ui.Button):
        """Pops up the modal to remove a link."""
        modal = RemoveLinkModal(self)
        await interaction.response.send_modal(modal)

    @ui.button(label="Refresh List", style=discord.ButtonStyle.blurple, emoji="🔄")
    async def refresh_list(self, interaction: discord.Interaction, button: ui.Button):
        """Refreshes the embed with the latest data."""
        await interaction.response.defer(thinking=True, ephemeral=True)
        new_embed = await self.cog.generate_list_embed(interaction.guild)
        await self.message.edit(embed=new_embed)
        await interaction.followup.send("Panel refreshed!", ephemeral=True)

    async def on_timeout(self):
        """Disables buttons when the view times out."""
        for item in self.children:
            item.disabled = True
        try:
            await self.message.edit(view=self)
        except discord.NotFound:
            pass # Message was likely deleted


# --- THE MAIN COG ---
class InviteTracker(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.invite_cache = {}
        self.role_map_file = "invite_roles.json"
        self.invite_role_map = self.load_role_map()
        # --- FIX: Add a dictionary to hold a lock for each guild ---
        self.guild_locks = {}

    def load_role_map(self):
        """Loads the invite-role map from the JSON file."""
        if os.path.exists(self.role_map_file):
            try:
                with open(self.role_map_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print("Error reading invite_roles.json. File might be corrupt. Starting with a new map.")
                return {}
        return {}

    def save_role_map(self):
        """Saves the current invite-role map to the JSON file."""
        try:
            with open(self.role_map_file, 'w') as f:
                json.dump(self.invite_role_map, f, indent=4)
        except Exception as e:
            print(f"CRITICAL: Failed to save invite_roles.json! Error: {e}")

    async def fetch_and_cache_invites(self, guild):
        """Fetches all invites for a guild and caches their use count."""
        try:
            invites = await guild.invites()
            self.invite_cache[guild.id] = {invite.code: invite.uses for invite in invites}
            print(f"Successfully cached {len(invites)} invites for {guild.name}.")
        except discord.Forbidden:
            print(f"Error: Bot does not have 'Manage Server' permission in {guild.name} to fetch invites.")
        except Exception as e:
            print(f"An error occurred while caching invites for {guild.name}: {e}")

    @commands.Cog.listener()
    async def on_ready(self):
        print(f"InviteTracker Cog is ready. Caching invites for all guilds...")
        for guild in self.bot.guilds:
            # --- FIX: Create a lock for each guild on ready ---
            self.guild_locks.setdefault(guild.id, asyncio.Lock())
            await self.fetch_and_cache_invites(guild)
        print("Invite caching complete.")

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """Cache invites when the bot joins a new guild."""
        print(f"Bot joined {guild.name}. Caching invites...")
        # --- FIX: Create a lock for the new guild ---
        self.guild_locks.setdefault(guild.id, asyncio.Lock())
        await self.fetch_and_cache_invites(guild)
        guild_id_str = str(guild.id)
        if guild_id_str not in self.invite_role_map:
            self.invite_role_map[guild_id_str] = {}
            self.save_role_map()

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Tracks which invite a new member used and assigns a role."""
        guild = member.guild
        
        # --- FIX: Acquire the lock for this guild ---
        # This ensures only one on_member_join event can run at a time for this specific guild.
        lock = self.guild_locks.setdefault(guild.id, asyncio.Lock())
        
        async with lock:
            guild_id_str = str(guild.id)

            # Give Discord's API a moment to update the invite uses
            await asyncio.sleep(2)

            try:
                cached_invites = self.invite_cache.get(guild.id, {})
                new_invites = await guild.invites()
                new_invites_map = {invite.code: invite.uses for invite in new_invites}

                used_invite_code = None
                for code, uses in new_invites_map.items():
                    # Check if the invite is new or if its use count has increased
                    if code not in cached_invites or uses > cached_invites.get(code, 0):
                        used_invite_code = code
                        break
                
                # Update the cache *after* finding the invite
                self.invite_cache[guild.id] = new_invites_map

                if used_invite_code:
                    print(f"Member {member.name} joined {guild.name} using invite code: {used_invite_code}")

                    guild_map = self.invite_role_map.get(guild_id_str, {})
                    role_id = guild_map.get(used_invite_code)

                    if role_id:
                        role = guild.get_role(role_id)
                        if role:
                            try:
                                await member.add_roles(role, reason=f"Joined via invite code {used_invite_code}")
                                print(f"Assigned role '{role.name}' to {member.name}.")
                            except discord.Forbidden:
                                print(f"Error: Bot does not have 'Manage Roles' permission or role '{role.name}' is higher than the bot's role.")
                            except Exception as e:
                                print(f"Error assigning role: {e}")
                        else:
                            print(f"Warning: Role ID {role_id} (from code {used_invite_code}) not found in server {guild.name}. It may have been deleted.")
                            del self.invite_role_map[guild_id_str][used_invite_code]
                            self.save_role_map()
                    else:
                        print(f"Invite code {used_invite_code} has no role mapped.")
                else:
                    print(f"Could not determine which invite {member.name} used to join {guild.name}.")

            except discord.Forbidden:
                print(f"Error: Bot does not have 'Manage Server' permission in {guild.name} to check invites on member join.")
            except Exception as e:
                print(f"An error occurred in on_member_join: {e}")
        
        # --- The lock is automatically released here ---

    # --- HELPER TO BUILD THE LIST EMBED ---
    async def generate_list_embed(self, guild: discord.Guild) -> discord.Embed:
        """Generates the embed showing all current invite-role links."""
        guild_id_str = str(guild.id)
        guild_map = self.invite_role_map.get(guild_id_str, {})

        embed = discord.Embed(
            title=f"Invite Role Links for {guild.name}",
            color=discord.Color.blue()
        )

        if not guild_map:
            embed.description = "ℹ️ This server has no invite-to-role links set up.\nClick 'Add/Update Link' to create one!"
        else:
            description = []
            for code, role_id in guild_map.items():
                role = guild.get_role(role_id)
                if role:
                    description.append(f"`{code}` ➔ {role.mention}")
                else:
                    description.append(f"`{code}` ➔ `[Deleted Role: ID {role_id}]`")
            embed.description = "\n".join(description)
        
        embed.set_footer(text="Use the buttons below to manage links.")
        return embed

    # --- THE NEW SINGLE SLASH COMMAND ---
    @app_commands.command(name="inviterolepanel", description="Manage invite-to-role links for this server.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def inviterolepanel(self, interaction: discord.Interaction):
        """Displays the interactive admin panel."""
        await interaction.response.defer(ephemeral=True)
        
        embed = await self.generate_list_embed(interaction.guild)
        view = InvitePanelView(self)
        
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        
        # Store the message on the view so we can edit it later
        view.message = await interaction.original_response()

    # --- ERROR HANDLER for the panel command ---
    @inviterolepanel.error
    async def on_inviterolepanel_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        """Catches errors from the panel slash command."""
        if isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message("❌ You must have the **Manage Server** permission to use this command.", ephemeral=True)
        else:
            print(f"An error occurred in the inviterolepanel command: {error}")
            if not interaction.response.is_done():
                await interaction.response.send_message("An unexpected error occurred. Please check the bot's console.", ephemeral=True)


# This is the required setup function that loads the cog
async def setup(bot):
    await bot.add_cog(InviteTracker(bot))


