import discord
from discord import app_commands, ui
from discord.ext import commands, tasks
import aiosqlite
import logging
import io
import textwrap
import datetime
from PIL import Image, ImageDraw, ImageFont
import os

# --- CONFIGURATION ---
DB_NAME = "intro_system.db"
# Font paths - DejaVu Bold for clean look
FONT_PATH_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

# Fallback
if not os.path.exists(FONT_PATH_BOLD):
    FONT_PATH_BOLD = "arialbd.ttf"

# Soft pastel accent colors for intro images (rotates per user)
ACCENT_COLORS = [
    (206, 236, 192),  # #CEECC0 - Soft lime
    (192, 236, 216),  # #C0ECD8 - Mint
    (192, 219, 236),  # #C0DBEC - Soft blue
    (203, 192, 236),  # #CBC0EC - Soft purple
    (236, 192, 231),  # #ECC0E7 - Soft pink
    (236, 192, 193),  # #ECC0C1 - Soft coral
    (236, 228, 192),  # #ECE4C0 - Soft cream
]

logger = logging.getLogger('bot_main')

class IntroCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_path = DB_NAME
        self.bot.loop.create_task(self.init_db())
        self.monthly_reset.start()

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')
            
            # Updated: Added is_optional column logic
            await db.execute('''
                CREATE TABLE IF NOT EXISTS questions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    text TEXT,
                    style TEXT,
                    order_num INTEGER,
                    is_optional INTEGER DEFAULT 0
                )
            ''')
            # Check if column exists, if not add it (Migration for existing DBs)
            try:
                await db.execute("ALTER TABLE questions ADD COLUMN is_optional INTEGER DEFAULT 0")
            except Exception:
                pass

            await db.execute('''
                CREATE TABLE IF NOT EXISTS role_config (
                    base_rank INTEGER,
                    tier INTEGER,
                    role_id INTEGER,
                    PRIMARY KEY (base_rank, tier)
                )
            ''')
            await db.execute('CREATE TABLE IF NOT EXISTS base_roles (base_rank INTEGER PRIMARY KEY, role_id INTEGER)')
            await db.execute('CREATE TABLE IF NOT EXISTS point_config (tier INTEGER PRIMARY KEY, points_required INTEGER)')
            await db.execute('CREATE TABLE IF NOT EXISTS user_points (user_id INTEGER PRIMARY KEY, points INTEGER DEFAULT 0)')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS thread_logs (
                    user_id INTEGER,
                    thread_id INTEGER,
                    msg_count INTEGER DEFAULT 0,
                    points_earned INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, thread_id)
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS point_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    month_str TEXT,
                    points INTEGER
                )
            ''')
            await db.execute('CREATE TABLE IF NOT EXISTS blacklist (user_id INTEGER PRIMARY KEY)')
            
            # New Table: Metadata for threads to handle deletion
            await db.execute('''
                CREATE TABLE IF NOT EXISTS intro_metadata (
                    thread_id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    lore_msg_id INTEGER,
                    parent_channel_id INTEGER
                )
            ''')

            # Hourly point tracking for rate limiting
            await db.execute('''
                CREATE TABLE IF NOT EXISTS hourly_points (
                    user_id INTEGER,
                    hour_key TEXT,
                    points_earned INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, hour_key)
                )
            ''')
            
            await db.commit()

    # --- IMAGE GENERATION ---
    def generate_lore_banner(self, username, user_id=0):
        """Generates the RPG Style Banner - high resolution for sharp text"""
        # Render at 4x scale for crisp text
        SCALE = 4
        W, H = 400 * SCALE, 70 * SCALE  # 1600x280
        bg_color = (43, 45, 49)
        accent_color = ACCENT_COLORS[user_id % len(ACCENT_COLORS)]

        img = Image.new('RGB', (W, H), color=bg_color)
        draw = ImageDraw.Draw(img)
        draw.rectangle([(0, 0), (6 * SCALE, H)], fill=accent_color)

        try:
            font_lg = ImageFont.truetype(FONT_PATH_BOLD, 20 * SCALE)  # 60pt
            font_sm = ImageFont.truetype(FONT_PATH_BOLD, 12 * SCALE)  # 36pt
        except Exception:
            font_lg = ImageFont.load_default()
            font_sm = ImageFont.load_default()

        # Center vertically (scaled positions)
        draw.text((20 * SCALE, 8 * SCALE), f"{username.upper()}'S", font=font_sm, fill="white")
        draw.text((20 * SCALE, 26 * SCALE), "LORE DROP", font=font_lg, fill=accent_color)

        return img

    def generate_qa_image(self, qa_list, avatar_bytes=None, username="User", user_id=0):
        """Generates High Quality Infographic List with user header - rendered at 3x for sharp text"""
        # qa_list = [(Question, Answer), ...]
        SCALE = 3  # Render at 3x for crisp text
        W = 700 * SCALE
        padding = 20 * SCALE
        row_padding = 15 * SCALE

        # Get accent color based on user_id
        accent_color = ACCENT_COLORS[user_id % len(ACCENT_COLORS)]

        # Header dimensions for avatar + username
        avatar_size = 40 * SCALE
        header_height = avatar_size + (20 * SCALE)

        try:
            font_q = ImageFont.truetype(FONT_PATH_BOLD, 18 * SCALE)
            font_a = ImageFont.truetype(FONT_PATH_BOLD, 14 * SCALE)
            font_username = ImageFont.truetype(FONT_PATH_BOLD, 20 * SCALE)
        except Exception:
            font_q = ImageFont.load_default()
            font_a = ImageFont.load_default()
            font_username = ImageFont.load_default()

        rows = []
        total_h = padding + header_height  # Start after header
        draw_temp = ImageDraw.Draw(Image.new('RGB', (1, 1)))

        for q, a in qa_list:
            # Skip if empty answer (double check)
            if not a or not a.strip():
                continue

            char_width = 10 * SCALE
            wrap_width = int((W - (padding * 2)) / char_width) + 10
            wrapped_a = textwrap.fill(a, width=wrap_width)

            bbox_q = draw_temp.textbbox((0, 0), q, font=font_q)
            h_q = bbox_q[3] - bbox_q[1]

            bbox_a = draw_temp.textbbox((0, 0), wrapped_a, font=font_a)
            h_a = bbox_a[3] - bbox_a[1]

            row_h = h_q + h_a + (row_padding * 2) + (10 * SCALE)
            rows.append({'q': q, 'a': wrapped_a, 'h': row_h, 'h_q': h_q})
            total_h += row_h

        total_h += padding

        if not rows:
            # Fallback if somehow empty
            total_h = (100 * SCALE) + header_height

        img = Image.new('RGB', (W, total_h), color=(30, 31, 34))
        draw = ImageDraw.Draw(img)

        # Draw user header (avatar + username)
        avatar_x = padding
        avatar_y = padding

        if avatar_bytes:
            try:
                avatar_img = Image.open(io.BytesIO(avatar_bytes)).convert("RGBA")
                avatar_img = avatar_img.resize((avatar_size, avatar_size), Image.Resampling.LANCZOS)

                # Create circular mask
                mask = Image.new("L", (avatar_size, avatar_size), 0)
                mask_draw = ImageDraw.Draw(mask)
                mask_draw.ellipse((0, 0, avatar_size, avatar_size), fill=255)

                # Create circular avatar
                circular_avatar = Image.new("RGBA", (avatar_size, avatar_size), (0, 0, 0, 0))
                circular_avatar.paste(avatar_img, (0, 0), mask)

                # Paste onto main image
                img.paste(circular_avatar, (avatar_x, avatar_y), circular_avatar)
            except Exception:
                # Draw placeholder circle if avatar fails
                draw.ellipse((avatar_x, avatar_y, avatar_x + avatar_size, avatar_y + avatar_size),
                            fill=accent_color)
        else:
            # Draw placeholder circle
            draw.ellipse((avatar_x, avatar_y, avatar_x + avatar_size, avatar_y + avatar_size),
                        fill=accent_color)

        # Draw username next to avatar
        username_x = avatar_x + avatar_size + (12 * SCALE)
        username_y = avatar_y + (avatar_size - (20 * SCALE)) // 2
        draw.text((username_x, username_y), username, font=font_username, fill="white")

        # Draw Q&A rows (starting after header)
        y = padding + header_height
        for i, row in enumerate(rows):
            if i % 2 == 0:
                draw.rectangle([(0, y), (W, y + row['h'])], fill=(43, 45, 49))

            draw.text((padding, y + row_padding), row['q'], font=font_q, fill=accent_color)
            draw.text((padding, y + row_padding + row['h_q'] + (8 * SCALE)), row['a'], font=font_a, fill="white")
            y += row['h']

        # Border outline with buffer for Discord corner rounding
        border_margin = 9 * SCALE
        draw.rectangle([(border_margin, border_margin), (W - 1 - border_margin, total_h - 1 - border_margin)], outline=accent_color, width=3 * SCALE)

        return img

    # --- ADMIN PANEL ---
    @app_commands.command(name="newcomer_panel", description="Admin: Configure the Intro System")
    async def newcomer_panel(self, interaction: discord.Interaction):
        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("Admin access only.", ephemeral=True)

        view = AdminPanelView(self)
        await interaction.response.send_message("**Newcomer Panel**\nSelect a module to configure:", view=view, ephemeral=True)

    @app_commands.command(name="regenerate_intro", description="Admin: Regenerate an intro image with new avatar/username header")
    @app_commands.describe(
        user="The user whose intro to regenerate",
        message_id="The message ID of the Q&A image in the intro channel"
    )
    async def regenerate_intro(self, interaction: discord.Interaction, user: discord.Member, message_id: str):
        if not self.bot.is_bot_admin(interaction.user):
            return await interaction.response.send_message("Admin access only.", ephemeral=True)

        # Store for the modal
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT value FROM settings WHERE key='intro_channel_id'")
            res = await cursor.fetchone()
            if not res:
                return await interaction.response.send_message("Intro channel not configured.", ephemeral=True)
            intro_channel_id = int(res[0])

        modal = RegenerateIntroModal(self, user, message_id, intro_channel_id)
        await interaction.response.send_modal(modal)

    # --- EVENTS ---
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type == discord.InteractionType.component:
            custom_id = interaction.data.get('custom_id')
            
            # INTRO BUTTON CLICK
            if custom_id == "start_intro_modal":
                async with aiosqlite.connect(self.db_path) as db:
                    cursor = await db.execute("SELECT 1 FROM blacklist WHERE user_id = ?", (interaction.user.id,))
                    if await cursor.fetchone():
                        return await interaction.response.send_message("You are blocked from using this.", ephemeral=True)

                    # Check for duplicate intro (admins can bypass)
                    if not self.bot.is_bot_admin(interaction.user):
                        cursor = await db.execute("SELECT 1 FROM intro_metadata WHERE user_id = ?", (interaction.user.id,))
                        if await cursor.fetchone():
                            return await interaction.response.send_message("You've already created an intro. Only one intro per user is allowed.", ephemeral=True)

                    # Fetch questions
                    cursor = await db.execute("SELECT text, style, is_optional FROM questions ORDER BY order_num ASC")
                    questions = await cursor.fetchall()
                
                if not questions:
                    return await interaction.response.send_message("No questions configured yet.", ephemeral=True)
                
                if len(questions) > 5:
                    questions = questions[:5] 

                modal = DynamicIntroModal(self, questions)
                await interaction.response.send_modal(modal)

            # CLOSE THREAD BUTTON CLICK
            elif custom_id and custom_id.startswith("close_thread_btn"):
                # Check DB for ownership
                thread_id = interaction.channel_id
                async with aiosqlite.connect(self.db_path) as db:
                    cursor = await db.execute("SELECT user_id, lore_msg_id, parent_channel_id FROM intro_metadata WHERE thread_id = ?", (thread_id,))
                    row = await cursor.fetchone()
                
                if not row:
                    # Fallback check if DB entry missing
                    # Fix: Added strict type check to prevent AttributeError on owner_id
                    is_owner = False
                    if isinstance(interaction.channel, discord.Thread) and interaction.channel.owner_id == interaction.user.id:
                        is_owner = True

                    if is_owner or self.bot.is_bot_admin(interaction.user):
                         # Allow confirm without deleting lore msg (cant find it)
                         return await interaction.response.send_message("Are you sure you want to close this thread?", view=CloseThreadConfirmView(self, None, None), ephemeral=True)
                    return await interaction.response.send_message("❌ Cannot verify thread ownership.", ephemeral=True)

                owner_id, lore_msg_id, parent_channel_id = row
                
                if interaction.user.id != owner_id and not self.bot.is_bot_admin(interaction.user):
                    return await interaction.response.send_message("❌ Only the thread owner or Admins can close this.", ephemeral=True)
                
                # Show confirmation
                view = CloseThreadConfirmView(self, lore_msg_id, parent_channel_id)
                await interaction.response.send_message("⚠️ **Are you sure?**\nThis will delete this thread AND the Lore Drop banner in the main channel.", view=view, ephemeral=True)


    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return

        # Only process messages in threads
        if not isinstance(message.channel, discord.Thread):
            return

        # Intro Thread Points Logic
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT value FROM settings WHERE key = 'thread_channel_id'")
            row = await cursor.fetchone()
            if not row:
                return
            thread_parent_id = int(row[0])

            # Must be a thread under the intro channel
            if message.channel.parent_id != thread_parent_id:
                return

            if not message.content or len(message.content) < 3:
                return

            # Filter out 1-word messages (low effort)
            if len(message.content.split()) < 2:
                return

            user_id = message.author.id
            thread_id = message.channel.id

            # Check if user is the thread owner (intro creator) - they can't earn points in their own thread
            cursor = await db.execute("SELECT user_id FROM intro_metadata WHERE thread_id = ?", (thread_id,))
            owner_row = await cursor.fetchone()
            if owner_row and owner_row[0] == user_id:
                return  # Thread owner cannot earn points in their own thread

            # Check hourly cap
            hour_key = datetime.datetime.now().strftime("%Y-%m-%d-%H")
            cursor = await db.execute("SELECT value FROM settings WHERE key = 'hourly_point_cap'")
            cap_row = await cursor.fetchone()
            hourly_cap = int(cap_row[0]) if cap_row else 0  # 0 = no cap

            if hourly_cap > 0:
                cursor = await db.execute("SELECT points_earned FROM hourly_points WHERE user_id = ? AND hour_key = ?", (user_id, hour_key))
                hourly_row = await cursor.fetchone()
                hourly_earned = hourly_row[0] if hourly_row else 0
                if hourly_earned >= hourly_cap:
                    return  # User has hit the hourly cap

            cursor = await db.execute("SELECT msg_count, points_earned FROM thread_logs WHERE user_id = ? AND thread_id = ?", (user_id, thread_id))
            log = await cursor.fetchone()

            points_to_add = 0
            if not log:
                await db.execute("INSERT INTO thread_logs (user_id, thread_id, msg_count, points_earned) VALUES (?, ?, 1, 3)", (user_id, thread_id))
                points_to_add = 3
            else:
                count, earned = log
                if earned < 5:
                    new_points = 1
                    if earned + new_points > 5:
                        new_points = 5 - earned
                    if new_points > 0:
                        await db.execute("UPDATE thread_logs SET msg_count = msg_count + 1, points_earned = points_earned + ? WHERE user_id = ? AND thread_id = ?", (new_points, user_id, thread_id))
                        points_to_add = new_points

            # Apply hourly cap limit to points_to_add if needed
            if hourly_cap > 0 and points_to_add > 0:
                cursor = await db.execute("SELECT points_earned FROM hourly_points WHERE user_id = ? AND hour_key = ?", (user_id, hour_key))
                hourly_row = await cursor.fetchone()
                hourly_earned = hourly_row[0] if hourly_row else 0
                remaining = hourly_cap - hourly_earned
                if points_to_add > remaining:
                    points_to_add = remaining

            if points_to_add > 0:
                await db.execute("INSERT INTO user_points (user_id, points) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET points = points + ?", (user_id, points_to_add, points_to_add))
                # Track hourly points
                await db.execute("INSERT INTO hourly_points (user_id, hour_key, points_earned) VALUES (?, ?, ?) ON CONFLICT(user_id, hour_key) DO UPDATE SET points_earned = points_earned + ?", (user_id, hour_key, points_to_add, points_to_add))
                await db.commit()
                await self.check_role_upgrade(message.author, db)

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        """Clean up all intro data when a user leaves the server"""
        async with aiosqlite.connect(self.db_path) as db:
            # Get intro metadata for this user
            cursor = await db.execute(
                "SELECT thread_id, lore_msg_id, parent_channel_id FROM intro_metadata WHERE user_id = ?",
                (member.id,)
            )
            row = await cursor.fetchone()

            if row:
                thread_id, lore_msg_id, parent_channel_id = row

                # Delete the lore banner message from parent channel
                if parent_channel_id and lore_msg_id:
                    try:
                        parent_ch = member.guild.get_channel(parent_channel_id)
                        if not parent_ch:
                            parent_ch = await member.guild.fetch_channel(parent_channel_id)
                        msg = await parent_ch.fetch_message(lore_msg_id)
                        await msg.delete()
                    except Exception as e:
                        logger.warning(f"Could not delete lore message for {member.id}: {e}")

                # Delete the thread
                if thread_id:
                    try:
                        thread = member.guild.get_channel(thread_id)
                        if not thread:
                            thread = await member.guild.fetch_channel(thread_id)
                        await thread.delete()
                    except Exception as e:
                        logger.warning(f"Could not delete thread for {member.id}: {e}")

                # Clean up database entries
                await db.execute("DELETE FROM intro_metadata WHERE user_id = ?", (member.id,))

            # Clean up other user data from this cog
            await db.execute("DELETE FROM thread_logs WHERE user_id = ?", (member.id,))
            await db.execute("DELETE FROM user_points WHERE user_id = ?", (member.id,))
            await db.execute("DELETE FROM hourly_points WHERE user_id = ?", (member.id,))
            await db.execute("DELETE FROM blacklist WHERE user_id = ?", (member.id,))
            await db.commit()

            logger.info(f"Cleaned up intro data for departed member {member.name} ({member.id})")

    async def check_role_upgrade(self, member, db):
        cursor = await db.execute("SELECT points FROM user_points WHERE user_id = ?", (member.id,))
        row = await cursor.fetchone()
        points = row[0] if row else 0

        thresholds = {} 
        cursor = await db.execute("SELECT tier, points_required FROM point_config")
        async for row in cursor: thresholds[row[0]] = row[1]
        
        if not thresholds: return

        target_tier = 0
        if points >= thresholds.get(3, 9999): target_tier = 3
        elif points >= thresholds.get(2, 9999): target_tier = 2
        elif points >= thresholds.get(1, 9999): target_tier = 1
        
        if target_tier == 0: return

        base_map = {} 
        cursor = await db.execute("SELECT base_rank, role_id FROM base_roles")
        async for row in cursor: base_map[row[0]] = row[1]
        
        user_base_rank = 0
        for rank in range(5, 0, -1):
            r_id = base_map.get(rank)
            if r_id and member.get_role(r_id):
                user_base_rank = rank
                break
        
        if user_base_rank == 0: return 
            
        cursor = await db.execute("SELECT role_id FROM role_config WHERE base_rank = ? AND tier = ?", (user_base_rank, target_tier))
        reward = await cursor.fetchone()

        if reward:
            reward_role = member.guild.get_role(reward[0])
            if reward_role and reward_role not in member.roles:
                try:
                    # Remove previous tier role if upgrading (1->2 or 2->3)
                    if target_tier > 1:
                        prev_tier = target_tier - 1
                        cursor = await db.execute("SELECT role_id FROM role_config WHERE base_rank = ? AND tier = ?", (user_base_rank, prev_tier))
                        prev_reward = await cursor.fetchone()
                        if prev_reward:
                            prev_role = member.guild.get_role(prev_reward[0])
                            if prev_role and prev_role in member.roles:
                                await member.remove_roles(prev_role)
                                logger.info(f"Removed Tier {prev_tier} role from {member.name}")

                    await member.add_roles(reward_role)
                    logger.info(f"Upgraded {member.name} to Tier {target_tier}")
                except Exception as e:
                    logger.error(f"Failed to assign role: {e}")

    async def repost_sticky_button(self, channel, db=None):
        """Delete old button message and repost at bottom"""
        close_db = False
        if db is None:
            db = await aiosqlite.connect(self.db_path)
            close_db = True

        try:
            cursor = await db.execute("SELECT value FROM settings WHERE key='intro_button_msg_id'")
            res = await cursor.fetchone()

            # Delete old button message
            if res:
                try:
                    old_msg = await channel.fetch_message(int(res[0]))
                    await old_msg.delete()
                except Exception:
                    pass  # Message already deleted or not found

            # Send new button
            new_msg = await channel.send("Click below to introduce yourself!", view=IntroButtonView())
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('intro_button_msg_id', ?)", (str(new_msg.id),))
            await db.commit()
        finally:
            if close_db:
                await db.close()

    @tasks.loop(hours=24)
    async def monthly_reset(self):
        now = datetime.datetime.now()
        if now.day == 1:
            async with aiosqlite.connect(self.db_path) as db:
                month_str = now.strftime("%Y-%m")
                cursor = await db.execute("SELECT 1 FROM point_history WHERE month_str = ? LIMIT 1", (month_str,))
                if await cursor.fetchone(): return

                await db.execute("INSERT INTO point_history (user_id, month_str, points) SELECT user_id, ?, points FROM user_points WHERE points > 0", (month_str,))
                await db.execute("DELETE FROM user_points")
                await db.execute("DELETE FROM thread_logs")
                await db.execute("DELETE FROM hourly_points")  # Clear hourly tracking on reset

                # Delete point history older than 3 months
                cutoff_year = now.year
                cutoff_month = now.month - 3
                while cutoff_month <= 0:
                    cutoff_month += 12
                    cutoff_year -= 1
                cutoff_str = f"{cutoff_year:04d}-{cutoff_month:02d}"
                await db.execute("DELETE FROM point_history WHERE month_str < ?", (cutoff_str,))

                await db.commit()

                # Role strip logic (simplified)
                cursor = await db.execute("SELECT role_id FROM role_config")
                all_tier_roles = [r[0] for r in await cursor.fetchall()]
                for guild in self.bot.guilds:
                    for member in guild.members:
                        to_remove = [r for r in member.roles if r.id in all_tier_roles]
                        if to_remove:
                            try: await member.remove_roles(*to_remove)
                            except Exception: pass
            logger.info("Monthly Reset Completed.")

# --- UI CLASSES ---

class DynamicIntroModal(ui.Modal):
    def __init__(self, cog, questions):
        super().__init__(title="Introduce Yourself")
        self.cog = cog
        self.questions = questions # List of (text, style, is_optional)
        
        for q_text, q_style, is_opt in questions:
            style = discord.TextStyle.paragraph if q_style == 'long' else discord.TextStyle.short
            required = True
            label = q_text
            placeholder = None
            
            if is_opt:
                required = False
                placeholder = "(Optional)"
            
            self.add_item(ui.TextInput(label=label[:45], style=style, required=required, placeholder=placeholder, max_length=1000))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        # 1. FIX: Grab IGN from first field explicitly, before filtering empty optionals
        # The list self.children corresponds exactly to questions order
        ign = "Newcomer"
        if self.children:
            ign = self.children[0].value.strip() or "Newcomer"

        # 2. Filter Answers (Skip empty optionals) for the image
        answers = []
        for i, item in enumerate(self.children):
            if item.value and item.value.strip():
                answers.append((item.label, item.value))
        
        # If absolutely everything is empty (shouldn't happen if name is req)
        if not answers:
             return await interaction.followup.send("You didn't fill anything out!", ephemeral=True)

        try:
            # Fetch user's avatar
            avatar_bytes = None
            try:
                avatar_asset = interaction.user.display_avatar
                avatar_bytes = await avatar_asset.read()
            except Exception:
                pass  # Will use placeholder if avatar fetch fails

            username = interaction.user.display_name
            user_id = interaction.user.id

            lore_img = await self.cog.bot.loop.run_in_executor(None, self.cog.generate_lore_banner, ign, user_id)
            qa_img = await self.cog.bot.loop.run_in_executor(
                None, self.cog.generate_qa_image, answers, avatar_bytes, username, user_id
            )
            
            # 3. FIX: BytesIO Context Manager Bug
            # Create buffers independently first (No nesting context managers for the send)
            bin_lore = io.BytesIO()
            lore_img.save(bin_lore, "PNG")
            bin_lore.seek(0)
            
            bin_qa = io.BytesIO()
            qa_img.save(bin_qa, "PNG")
            bin_qa.seek(0)
            
            # Create discord Files from the open buffers
            file_lore = discord.File(bin_lore, filename="lore.png")
            file_qa = discord.File(bin_qa, filename="qa.png")

            async with aiosqlite.connect(self.cog.db_path) as db:
                # Get Parent Channel
                cursor = await db.execute("SELECT value FROM settings WHERE key='thread_channel_id'")
                res = await cursor.fetchone()
                if not res: return await interaction.followup.send("Thread Parent Channel not configured!")
                
                parent_channel_id = int(res[0])
                parent_channel = interaction.guild.get_channel(parent_channel_id)
                if not parent_channel:
                    # Try fetch
                    try: parent_channel = await interaction.guild.fetch_channel(parent_channel_id)
                    except Exception: return await interaction.followup.send("Thread Parent Channel invalid!")

                # Send Hook (Lore Drop)
                hook_msg = await parent_channel.send(file=file_lore)
                
                # Create Thread (ID = hook_msg.id)
                thread = await hook_msg.create_thread(name=f"Welcome {ign}!", auto_archive_duration=1440) # 24h
                
                # Save Metadata for Deletion
                await db.execute('''
                    INSERT OR REPLACE INTO intro_metadata (thread_id, user_id, lore_msg_id, parent_channel_id) 
                    VALUES (?, ?, ?, ?)
                ''', (thread.id, interaction.user.id, hook_msg.id, parent_channel.id))
                await db.commit()

                # Send Welcome in Thread
                cursor = await db.execute("SELECT value FROM settings WHERE key='welcome_msg'")
                w_res = await cursor.fetchone()
                
                # Default Message
                w_default = (
                    "Thanks for the Lore Drop, {username}!\n\n"
                    "We’ve pinned your intro here so the welcome wagon can say hello without it getting lost in the main chat scroll.\n\n"
                    "**This space is totally optional.** Feel free to chat here, or if you prefer to just jump into the main channels, "
                    "you can delete this thread instantly using the **Close Thread** button below."
                )
                
                w_msg = w_res[0] if w_res else w_default
                w_msg = w_msg.replace("{username}", interaction.user.mention)

                close_view = CloseThreadView()
                await thread.send(content=w_msg, file=file_qa, view=close_view)

                # Send Q&A to intro channel as well
                cursor = await db.execute("SELECT value FROM settings WHERE key='intro_channel_id'")
                intro_res = await cursor.fetchone()
                if intro_res:
                    intro_channel = interaction.guild.get_channel(int(intro_res[0]))
                    if not intro_channel:
                        try: intro_channel = await interaction.guild.fetch_channel(int(intro_res[0]))
                        except Exception: intro_channel = None

                    if intro_channel:
                        # Re-create file since discord.File can only be used once
                        bin_qa2 = io.BytesIO()
                        qa_img.save(bin_qa2, "PNG")
                        bin_qa2.seek(0)
                        file_qa2 = discord.File(bin_qa2, filename="qa.png")
                        await intro_channel.send(f"**{ign}** just introduced themselves!", file=file_qa2)

                        # Repost sticky button
                        await self.cog.repost_sticky_button(intro_channel, db)

                await interaction.followup.send("✅ Introduction posted!", ephemeral=True)

        except Exception as e:
            logger.error(f"Intro Fail: {e}", exc_info=True)
            await interaction.followup.send("Failed to generate intro.", ephemeral=True)


class RegenerateIntroModal(ui.Modal, title="Regenerate Intro"):
    """Modal for admins to regenerate an existing intro with new avatar format.
    Format: Q1|A1;;Q2|A2;;Q3|A3 (use ;; to separate Q&A pairs, | to separate Q from A)
    """
    qa_data = ui.TextInput(
        label="Q&A Data (Q1|A1;;Q2|A2;;...)",
        style=discord.TextStyle.paragraph,
        placeholder="What's your name?|OG;;What games?|Valorant;;Hot take?|Wolves are cool",
        max_length=2000
    )

    def __init__(self, cog, user: discord.Member, message_id: str, intro_channel_id: int):
        super().__init__()
        self.cog = cog
        self.user = user
        self.message_id = message_id
        self.intro_channel_id = intro_channel_id

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        # Parse Q&A data: Q1|A1;;Q2|A2;;Q3|A3
        qa_list = []
        pairs = self.qa_data.value.split(";;")
        for pair in pairs:
            if "|" in pair:
                parts = pair.split("|", 1)
                if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                    qa_list.append((parts[0].strip(), parts[1].strip()))

        if not qa_list:
            return await interaction.followup.send("You need at least one Q&A pair.", ephemeral=True)

        try:
            # Fetch user's avatar
            avatar_bytes = None
            try:
                avatar_asset = self.user.display_avatar
                avatar_bytes = await avatar_asset.read()
            except Exception:
                pass

            username = self.user.display_name

            # Generate new image
            qa_img = await self.cog.bot.loop.run_in_executor(
                None, self.cog.generate_qa_image, qa_list, avatar_bytes, username
            )

            bin_qa = io.BytesIO()
            qa_img.save(bin_qa, "PNG")
            bin_qa.seek(0)
            file_qa = discord.File(bin_qa, filename="qa.png")

            # Get the intro channel and edit the message
            intro_channel = interaction.guild.get_channel(self.intro_channel_id)
            if not intro_channel:
                intro_channel = await interaction.guild.fetch_channel(self.intro_channel_id)

            try:
                msg = await intro_channel.fetch_message(int(self.message_id))
                # Delete old message and send new one (can't edit attachments)
                await msg.delete()
                ign = qa_list[0][1].split()[0] if qa_list else username  # Use first word of first answer as IGN
                await intro_channel.send(f"**{username}** just introduced themselves!", file=file_qa)
                await interaction.followup.send(f"Intro regenerated for {self.user.mention}!", ephemeral=True)
            except discord.NotFound:
                return await interaction.followup.send(f"Message ID {self.message_id} not found in the intro channel.", ephemeral=True)

        except Exception as e:
            logger.error(f"Regenerate Intro Fail: {e}", exc_info=True)
            await interaction.followup.send(f"Failed to regenerate: {e}", ephemeral=True)


# --- THREAD CONTROL VIEWS ---
class CloseThreadView(ui.View):
    def __init__(self):
        super().__init__(timeout=None) # Persistent
    
    @ui.button(label="Close Thread", style=discord.ButtonStyle.danger, emoji="🔒", custom_id="close_thread_btn")
    async def close_btn(self, interaction, button):
        pass # Handled in Cog listener

class CloseThreadConfirmView(ui.View):
    def __init__(self, cog, lore_msg_id, parent_channel_id):
        super().__init__(timeout=60)
        self.cog = cog
        self.lore_msg_id = lore_msg_id
        self.parent_channel_id = parent_channel_id

    @ui.button(label="Yes, Close it", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction, button):
        await interaction.response.defer()
        
        # 1. Delete Lore Message
        if self.parent_channel_id and self.lore_msg_id:
            try:
                parent_ch = interaction.guild.get_channel(self.parent_channel_id)
                if not parent_ch:
                    parent_ch = await interaction.guild.fetch_channel(self.parent_channel_id)
                
                msg = await parent_ch.fetch_message(self.lore_msg_id)
                await msg.delete()
            except Exception as e:
                logger.warning(f"Could not delete lore message: {e}")

        # 2. Delete Thread
        try:
            await interaction.channel.delete()
        except Exception as e:
            await interaction.followup.send(f"Failed to delete thread: {e}", ephemeral=True)

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction, button):
        await interaction.response.edit_message(content="Cancelled.", view=None)

# --- ADMIN VIEWS ---
class AddQuestionModal(ui.Modal, title="Add Question"):
    order = ui.TextInput(label="Order (1-5)", max_length=1)
    q_text = ui.TextInput(label="Question Text", max_length=45)
    style_in = ui.TextInput(label="Style (short/long)", placeholder="short/long")
    is_opt = ui.TextInput(label="Optional?", placeholder="yes/no", max_length=3)

    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction):
        if self.style_in.value.lower() not in ['short', 'long']:
            return await interaction.response.send_message("Style must be 'short' or 'long'", ephemeral=True)
        
        is_optional = 1 if self.is_opt.value.lower() in ['yes', 'y', 'true'] else 0

        try:
            o_num = int(self.order.value)
            if not 1 <= o_num <= 5: raise ValueError
        except Exception:
             return await interaction.response.send_message("Order must be 1-5", ephemeral=True)

        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT INTO questions (text, style, order_num, is_optional) VALUES (?, ?, ?, ?)", 
                             (self.q_text.value, self.style_in.value.lower(), o_num, is_optional))
            await db.commit()
        await interaction.response.send_message("Question added.", ephemeral=True)

class AdminPanelView(ui.View):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    @ui.button(label="Channels", style=discord.ButtonStyle.primary, row=0)
    async def channels_btn(self, interaction, button):
        await interaction.response.send_message("Select Channels:", view=ChannelSelectView(self.cog), ephemeral=True)

    @ui.button(label="Questions", style=discord.ButtonStyle.primary, row=0)
    async def questions_btn(self, interaction, button):
        await interaction.response.send_message("Manage Questions:", view=QuestionManagerView(self.cog), ephemeral=True)

    @ui.button(label="Role Config", style=discord.ButtonStyle.secondary, row=1)
    async def roles_btn(self, interaction, button):
        await interaction.response.send_message("Configure Roles:", view=RoleConfigMainView(self.cog), ephemeral=True)

    @ui.button(label="Points/Tiers", style=discord.ButtonStyle.secondary, row=1)
    async def points_btn(self, interaction, button):
        await interaction.response.send_modal(PointThresholdModal(self.cog))

    @ui.button(label="Hourly Cap", style=discord.ButtonStyle.secondary, row=1)
    async def hourly_cap_btn(self, interaction, button):
        await interaction.response.send_modal(HourlyCapModal(self.cog))
    
    @ui.button(label="Blacklist", style=discord.ButtonStyle.danger, row=2)
    async def bl_btn(self, interaction, button):
        await interaction.response.send_message("Select User to Block:", view=UserSelectView(self.cog, "blacklist"), ephemeral=True)

    @ui.button(label="History", style=discord.ButtonStyle.secondary, row=2)
    async def history_btn(self, interaction, button):
        view = HistoryMonthSelectView(self.cog)
        await interaction.response.send_message("**Select a month to view:**", view=view, ephemeral=True)

    @ui.button(label="Welcome Message", style=discord.ButtonStyle.success, row=2)
    async def msg_btn(self, interaction, button):
        await interaction.response.send_modal(WelcomeMsgModal(self.cog))

    @ui.button(label="Wipe Points", style=discord.ButtonStyle.danger, row=3)
    async def wipe_btn(self, interaction, button):
        await interaction.response.send_message("**Wipe Points**\nChoose an option:", view=WipeOptionsView(self.cog), ephemeral=True)

class IntroButtonView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @ui.button(label="Introduce Yourself", style=discord.ButtonStyle.success, emoji="👋", custom_id="start_intro_modal")
    async def intro_btn(self, interaction, button): pass

class ChannelSelectView(ui.View):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
    
    @ui.select(cls=ui.ChannelSelect, placeholder="Select Intro Channel (Button location)", min_values=1, max_values=1)
    async def sel_intro(self, interaction, select):
        ch_partial = select.values[0]
        # FIX: Fetch the full channel object to access .send()
        ch = interaction.guild.get_channel(ch_partial.id)
        if not ch:
            try: 
                ch = await interaction.guild.fetch_channel(ch_partial.id)
            except Exception: 
                return await interaction.response.send_message("Error fetching channel", ephemeral=True)

        try:
            btn_msg = await ch.send("Click below to introduce yourself!", view=IntroButtonView())
            async with aiosqlite.connect(self.cog.db_path) as db:
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('intro_channel_id', ?)", (str(ch.id),))
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('intro_button_msg_id', ?)", (str(btn_msg.id),))
                await db.commit()
            await interaction.response.send_message(f"Button sent to {ch.mention}", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Error: {e}", ephemeral=True)

    @ui.select(cls=ui.ChannelSelect, placeholder="Select Thread Parent Channel", min_values=1, max_values=1)
    async def sel_parent(self, interaction, select):
        ch_partial = select.values[0]
        ch = interaction.guild.get_channel(ch_partial.id)
        if not ch:
             try: ch = await interaction.guild.fetch_channel(ch_partial.id)
             except Exception: ch = ch_partial 

        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('thread_channel_id', ?)", (str(ch.id),))
            await db.commit()
        await interaction.response.send_message(f"Thread Parent set to {ch.mention}", ephemeral=True)

class QuestionManagerView(ui.View):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    @ui.button(label="Add Question", style=discord.ButtonStyle.success)
    async def add_q(self, interaction, button):
        await interaction.response.send_modal(AddQuestionModal(self.cog))

    @ui.button(label="Delete Question", style=discord.ButtonStyle.danger)
    async def del_q(self, interaction, button):
        async with aiosqlite.connect(self.cog.db_path) as db:
            cursor = await db.execute("SELECT id, text, order_num FROM questions ORDER BY order_num")
            qs = await cursor.fetchall()
        
        if not qs:
            return await interaction.response.send_message("No questions to delete.", ephemeral=True)
        view = DeleteQuestionSelectView(self.cog, qs)
        await interaction.response.send_message("Select question to delete:", view=view, ephemeral=True)

class DeleteQuestionSelectView(ui.View):
    def __init__(self, cog, questions):
        super().__init__()
        self.cog = cog
        options = [discord.SelectOption(label=f"{q[2]}. {q[1][:20]}", value=str(q[0])) for q in questions]
        self.select = ui.Select(placeholder="Choose question...", options=options)
        self.select.callback = self.callback
        self.add_item(self.select)
    
    async def callback(self, interaction):
        q_id = int(self.select.values[0])
        await interaction.response.send_message(f"Question {q_id} selected. Shift remaining questions up?", 
                                                view=ShiftConfirmView(self.cog, q_id), ephemeral=True)

class ShiftConfirmView(ui.View):
    def __init__(self, cog, q_id):
        super().__init__()
        self.cog = cog
        self.q_id = q_id

    @ui.button(label="Yes, Shift", style=discord.ButtonStyle.success)
    async def yes(self, interaction, button): await self.execute_delete(interaction, shift=True)

    @ui.button(label="No, Just Delete", style=discord.ButtonStyle.secondary)
    async def no(self, interaction, button): await self.execute_delete(interaction, shift=False)

    async def execute_delete(self, interaction, shift):
        async with aiosqlite.connect(self.cog.db_path) as db:
            cursor = await db.execute("SELECT order_num FROM questions WHERE id = ?", (self.q_id,))
            res = await cursor.fetchone()
            if not res: return
            del_order = res[0]
            await db.execute("DELETE FROM questions WHERE id = ?", (self.q_id,))
            if shift:
                await db.execute("UPDATE questions SET order_num = order_num - 1 WHERE order_num > ?", (del_order,))
            await db.commit()
        await interaction.response.send_message("Deleted and updated.", ephemeral=True)

class RoleConfigMainView(ui.View):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    @ui.select(cls=ui.RoleSelect, placeholder="Select BASE Role 1 (Lowest)", min_values=1, max_values=1)
    async def base1(self, interaction, select): await self.set_base(interaction, 1, select.values[0])
    @ui.select(cls=ui.RoleSelect, placeholder="Select BASE Role 2", min_values=1, max_values=1)
    async def base2(self, interaction, select): await self.set_base(interaction, 2, select.values[0])
    @ui.select(cls=ui.RoleSelect, placeholder="Select BASE Role 3", min_values=1, max_values=1)
    async def base3(self, interaction, select): await self.set_base(interaction, 3, select.values[0])
    @ui.select(cls=ui.RoleSelect, placeholder="Select BASE Role 4", min_values=1, max_values=1)
    async def base4(self, interaction, select): await self.set_base(interaction, 4, select.values[0])
    @ui.select(cls=ui.RoleSelect, placeholder="Select BASE Role 5 (Highest)", min_values=1, max_values=1)
    async def base5(self, interaction, select): await self.set_base(interaction, 5, select.values[0])

    async def set_base(self, interaction, rank, role):
        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO base_roles (base_rank, role_id) VALUES (?, ?)", (rank, role.id))
            await db.commit()
        await interaction.response.send_message(f"Base {rank} set to {role.name}. Now select Tiers:", view=TierConfigView(self.cog, rank), ephemeral=True)

class TierConfigView(ui.View):
    def __init__(self, cog, base_rank):
        super().__init__()
        self.cog = cog
        self.base_rank = base_rank
    @ui.select(cls=ui.RoleSelect, placeholder="Tier 1 Reward Role", min_values=1, max_values=1)
    async def t1(self, interaction, select): await self.set_tier(interaction, 1, select.values[0])
    @ui.select(cls=ui.RoleSelect, placeholder="Tier 2 Reward Role", min_values=1, max_values=1)
    async def t2(self, interaction, select): await self.set_tier(interaction, 2, select.values[0])
    @ui.select(cls=ui.RoleSelect, placeholder="Tier 3 Reward Role (Max)", min_values=1, max_values=1)
    async def t3(self, interaction, select): await self.set_tier(interaction, 3, select.values[0])

    async def set_tier(self, interaction, tier, role):
        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO role_config (base_rank, tier, role_id) VALUES (?, ?, ?)", (self.base_rank, tier, role.id))
            await db.commit()
        await interaction.response.send_message(f"Base {self.base_rank} Tier {tier} set to {role.name}", ephemeral=True)

class PointThresholdModal(ui.Modal, title="Points Required per Tier"):
    t1 = ui.TextInput(label="Tier 1 Points", max_length=4)
    t2 = ui.TextInput(label="Tier 2 Points", max_length=4)
    t3 = ui.TextInput(label="Tier 3 Points", max_length=4)
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
    async def on_submit(self, interaction):
        try:
            p1, p2, p3 = int(self.t1.value), int(self.t2.value), int(self.t3.value)
        except Exception: return await interaction.response.send_message("Must be numbers.", ephemeral=True)
        async with aiosqlite.connect(self.cog.db_path) as db:
            for t, p in [(1, p1), (2, p2), (3, p3)]:
                await db.execute("INSERT OR REPLACE INTO point_config (tier, points_required) VALUES (?, ?)", (t, p))
            await db.commit()
        await interaction.response.send_message("Thresholds updated.", ephemeral=True)

class HourlyCapModal(ui.Modal, title="Hourly Point Cap"):
    cap = ui.TextInput(label="Max Points Per Hour (0 = no limit)", max_length=4, placeholder="e.g. 10")

    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction):
        try:
            cap_val = int(self.cap.value)
            if cap_val < 0:
                raise ValueError
        except ValueError:
            return await interaction.response.send_message("Must be a positive number (or 0 for no limit).", ephemeral=True)

        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('hourly_point_cap', ?)", (str(cap_val),))
            await db.commit()

        if cap_val == 0:
            await interaction.response.send_message("Hourly cap disabled (no limit).", ephemeral=True)
        else:
            await interaction.response.send_message(f"Hourly cap set to **{cap_val}** points.", ephemeral=True)

class WelcomeMsgModal(ui.Modal, title="Set Welcome Message"):
    msg = ui.TextInput(label="Message ({username} to ping)", style=discord.TextStyle.paragraph)
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
    async def on_submit(self, interaction):
        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('welcome_msg', ?)", (self.msg.value,))
            await db.commit()
        await interaction.response.send_message("Welcome message saved.", ephemeral=True)

class UserSelectView(ui.View):
    def __init__(self, cog, mode):
        super().__init__()
        self.cog = cog
        self.mode = mode
    @ui.select(cls=ui.UserSelect, placeholder="Select User")
    async def callback(self, interaction, select):
        user = select.values[0]
        if self.mode == 'blacklist':
            async with aiosqlite.connect(self.cog.db_path) as db:
                await db.execute("INSERT OR IGNORE INTO blacklist (user_id) VALUES (?)", (user.id,))
                await db.commit()
            await interaction.response.send_message(f"{user.name} blacklisted.", ephemeral=True)

class WipeOptionsView(ui.View):
    """Main wipe options: Wipe All or Member Specific"""
    def __init__(self, cog):
        super().__init__(timeout=60)
        self.cog = cog

    @ui.button(label="Wipe All Members", style=discord.ButtonStyle.danger)
    async def wipe_all_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message(
            "⚠️ **Are you sure?**\nThis will wipe ALL points and thread logs for the current month.",
            view=WipeAllConfirmView(self.cog),
            ephemeral=True
        )

    @ui.button(label="Member Specific", style=discord.ButtonStyle.primary)
    async def member_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_message(
            "Select a member:",
            view=WipeMemberSelectView(self.cog),
            ephemeral=True
        )

class WipeAllConfirmView(ui.View):
    """Confirmation for wiping all points"""
    def __init__(self, cog):
        super().__init__(timeout=30)
        self.cog = cog

    @ui.button(label="Yes, Wipe Everything", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: ui.Button):
        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("DELETE FROM user_points")
            await db.execute("DELETE FROM thread_logs")
            await db.commit()
        await interaction.response.edit_message(content="All points and thread logs wiped for this month.", view=None)

    @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.edit_message(content="Cancelled.", view=None)

class WipeMemberSelectView(ui.View):
    """Select a member to modify points"""
    def __init__(self, cog):
        super().__init__(timeout=60)
        self.cog = cog

    @ui.select(cls=ui.UserSelect, placeholder="Select Member")
    async def select_member(self, interaction: discord.Interaction, select: ui.UserSelect):
        user = select.values[0]
        # Get current points
        async with aiosqlite.connect(self.cog.db_path) as db:
            cursor = await db.execute("SELECT points FROM user_points WHERE user_id = ?", (user.id,))
            row = await cursor.fetchone()
            current_points = row[0] if row else 0

        await interaction.response.edit_message(
            content=f"**{user.display_name}** - Current Points: **{current_points}**\nChoose an action:",
            view=WipeMemberActionView(self.cog, user, current_points)
        )

class WipeMemberActionView(ui.View):
    """Actions for a specific member's points"""
    def __init__(self, cog, user, current_points):
        super().__init__(timeout=60)
        self.cog = cog
        self.user = user
        self.current_points = current_points

    @ui.button(label="Add Points", style=discord.ButtonStyle.success)
    async def add_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(PointAmountModal(self.cog, self.user, "add"))

    @ui.button(label="Remove Points", style=discord.ButtonStyle.primary)
    async def remove_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(PointAmountModal(self.cog, self.user, "remove"))

    @ui.button(label="Set Points", style=discord.ButtonStyle.secondary)
    async def set_btn(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(PointAmountModal(self.cog, self.user, "set"))

    @ui.button(label="Wipe All", style=discord.ButtonStyle.danger)
    async def wipe_btn(self, interaction: discord.Interaction, button: ui.Button):
        async with aiosqlite.connect(self.cog.db_path) as db:
            await db.execute("DELETE FROM user_points WHERE user_id = ?", (self.user.id,))
            await db.execute("DELETE FROM thread_logs WHERE user_id = ?", (self.user.id,))
            await db.commit()
        await interaction.response.edit_message(
            content=f"Wiped all points and thread logs for **{self.user.display_name}**.",
            view=None
        )

class PointAmountModal(ui.Modal, title="Enter Amount"):
    amount = ui.TextInput(label="Points", placeholder="Enter a number", max_length=6)

    def __init__(self, cog, user, action):
        super().__init__()
        self.cog = cog
        self.user = user
        self.action = action  # "add", "remove", or "set"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            pts = int(self.amount.value)
            if pts < 0:
                raise ValueError
        except ValueError:
            return await interaction.response.send_message("Must be a positive number.", ephemeral=True)

        async with aiosqlite.connect(self.cog.db_path) as db:
            if self.action == "add":
                await db.execute(
                    "INSERT INTO user_points (user_id, points) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET points = points + ?",
                    (self.user.id, pts, pts)
                )
                msg = f"Added **{pts}** points to **{self.user.display_name}**."
            elif self.action == "remove":
                await db.execute(
                    "UPDATE user_points SET points = MAX(0, points - ?) WHERE user_id = ?",
                    (pts, self.user.id)
                )
                msg = f"Removed **{pts}** points from **{self.user.display_name}**."
            else:  # set
                await db.execute(
                    "INSERT INTO user_points (user_id, points) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET points = ?",
                    (self.user.id, pts, pts)
                )
                msg = f"Set **{self.user.display_name}**'s points to **{pts}**."
            await db.commit()

        await interaction.response.send_message(msg, ephemeral=True)

class HistoryMonthSelectView(ui.View):
    """View to select which month to view history for"""
    def __init__(self, cog):
        super().__init__(timeout=60)
        self.cog = cog

        # Build month options: current month + last 3 months
        now = datetime.datetime.now()
        options = []

        # Current month (active points)
        current_month = now.strftime("%Y-%m")
        options.append(discord.SelectOption(
            label=now.strftime("%B %Y") + " (Current)",
            value=f"current:{current_month}",
            description="View active points this month"
        ))

        # Previous 3 months
        for i in range(1, 4):
            # Go back i months
            year = now.year
            month = now.month - i
            while month <= 0:
                month += 12
                year -= 1
            past_date = datetime.datetime(year, month, 1)
            month_str = past_date.strftime("%Y-%m")
            options.append(discord.SelectOption(
                label=past_date.strftime("%B %Y"),
                value=f"history:{month_str}",
                description=f"View archived points for {past_date.strftime('%B %Y')}"
            ))

        self.select = ui.Select(placeholder="Select a month...", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        value = self.select.values[0]
        source, month_str = value.split(":", 1)

        # Fetch data based on source
        async with aiosqlite.connect(self.cog.db_path) as db:
            if source == "current":
                # Get from user_points (active)
                cursor = await db.execute(
                    "SELECT user_id, points FROM user_points WHERE points > 0 ORDER BY points DESC"
                )
            else:
                # Get from point_history (archived)
                cursor = await db.execute(
                    "SELECT user_id, points FROM point_history WHERE month_str = ? ORDER BY points DESC",
                    (month_str,)
                )
            rows = await cursor.fetchall()

        if not rows:
            return await interaction.response.edit_message(content="No point data for this month.", view=None)

        # Parse month for display
        try:
            display_date = datetime.datetime.strptime(month_str, "%Y-%m")
            display_name = display_date.strftime("%B %Y")
            if source == "current":
                display_name += " (Active)"
        except:
            display_name = month_str

        # Show paginated view
        view = HistoryPaginatedView(self.cog, rows, display_name, interaction.guild)
        await interaction.response.edit_message(content=await view.build_page(), view=view)

class HistoryPaginatedView(ui.View):
    """Paginated view for showing point history leaderboard"""
    ITEMS_PER_PAGE = 10

    def __init__(self, cog, data, month_name, guild):
        super().__init__(timeout=120)
        self.cog = cog
        self.data = data  # List of (user_id, points)
        self.month_name = month_name
        self.guild = guild
        self.page = 0
        self.max_page = max(0, (len(data) - 1) // self.ITEMS_PER_PAGE)
        self.update_buttons()

    def update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.max_page

    async def build_page(self):
        start = self.page * self.ITEMS_PER_PAGE
        end = start + self.ITEMS_PER_PAGE
        page_data = self.data[start:end]

        lines = [f"**{self.month_name} - Points Leaderboard**\n"]

        for i, (user_id, points) in enumerate(page_data, start=start + 1):
            # Try to get member name
            member = self.guild.get_member(user_id)
            if member:
                name = member.display_name
            else:
                name = f"User {user_id}"
            lines.append(f"`{i}.` **{name}** - {points} pts")

        lines.append(f"\nPage {self.page + 1}/{self.max_page + 1}")
        return "\n".join(lines)

    @ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary, row=0)
    async def prev_btn(self, interaction: discord.Interaction, button: ui.Button):
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(content=await self.build_page(), view=self)

    @ui.button(label="Next ▶", style=discord.ButtonStyle.secondary, row=0)
    async def next_btn(self, interaction: discord.Interaction, button: ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(content=await self.build_page(), view=self)

async def setup(bot):
    await bot.add_cog(IntroCog(bot))


