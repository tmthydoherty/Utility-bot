import discord
from discord.ext import commands, tasks
import json
import os
from datetime import datetime, date, time
import zoneinfo
import logging

logger = logging.getLogger("cogs.birthday")

BIRTHDAY_FILE = "data/birthdays.json"
ANNOUNCE_CHANNEL_ID = 1431565436713177209
CHICAGO_TZ = zoneinfo.ZoneInfo("America/Chicago")

def load_birthdays():
    if os.path.exists(BIRTHDAY_FILE):
        try:
            with open(BIRTHDAY_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}

def save_birthdays(data):
    # Ensure data directory exists
    os.makedirs(os.path.dirname(BIRTHDAY_FILE), exist_ok=True)
    with open(BIRTHDAY_FILE, 'w') as f:
        json.dump(data, f, indent=4)

class BirthdayModal(discord.ui.Modal, title="Add Your Birthday"):
    month = discord.ui.TextInput(
        label="Month (MM)",
        placeholder="e.g. 10 for October",
        min_length=1,
        max_length=2,
        required=True
    )
    day = discord.ui.TextInput(
        label="Day (DD)",
        placeholder="e.g. 25",
        min_length=1,
        max_length=2,
        required=True
    )
    year = discord.ui.TextInput(
        label="Year (YYYY) (Optional)",
        placeholder="e.g. 1990",
        min_length=4,
        max_length=4,
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            m = int(self.month.value)
            d = int(self.day.value)
            y = int(self.year.value) if self.year.value.strip() else None
            
            # Validate date
            if y is not None:
                date(y, m, d)
            else:
                date(2020, m, d) # 2020 is a leap year, so Feb 29 is valid
                
        except ValueError:
            await interaction.response.send_message("Invalid date provided. Please check your inputs and try again.", ephemeral=True)
            return

        data = load_birthdays()
        data[str(interaction.user.id)] = {"month": m, "day": d, "year": y}
        save_birthdays(data)
        
        await interaction.response.edit_message(content="Your birthday has been remembered! 🎂", view=BirthdayView())

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"Error in BirthdayModal: {error}", exc_info=error)
        if not interaction.response.is_done():
            await interaction.response.send_message("An error occurred. Please try again.", ephemeral=True)


class BirthdayView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Remember birthday", style=discord.ButtonStyle.primary, custom_id="bday_remember")
    async def remember(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(BirthdayModal())

    @discord.ui.button(label="Forget birthday", style=discord.ButtonStyle.danger, custom_id="bday_forget")
    async def forget(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = load_birthdays()
        user_id = str(interaction.user.id)
        if user_id in data:
            del data[user_id]
            save_birthdays(data)
            await interaction.response.edit_message(content="Your birthday has been forgotten.", view=self)
        else:
            await interaction.response.edit_message(content="I didn't have your birthday saved anyway.", view=self)

    @discord.ui.button(label="Next birthdays", style=discord.ButtonStyle.secondary, custom_id="bday_next")
    async def next_bdays(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = load_birthdays()
        if not data:
            await interaction.response.edit_message(content="No birthdays have been remembered yet!", view=self)
            return

        now = datetime.now(CHICAGO_TZ).date()
        upcoming = []
        
        def get_next_occurrence(m, d):
            try:
                dt = date(now.year, m, d)
            except ValueError:
                # Leap year edge case for non-leap year
                if m == 2 and d == 29:
                    dt = date(now.year, 3, 1)
                else:
                    return None
            
            if dt < now:
                try:
                    dt = date(now.year + 1, m, d)
                except ValueError:
                    if m == 2 and d == 29:
                        dt = date(now.year + 1, 3, 1)
            return dt

        for uid_str, binfo in data.items():
            dt = get_next_occurrence(binfo["month"], binfo["day"])
            if dt:
                upcoming.append((dt, int(uid_str), binfo["year"]))

        if not upcoming:
            await interaction.response.edit_message(content="No upcoming birthdays found.", view=self)
            return

        upcoming.sort(key=lambda x: x[0])
        top_10 = upcoming[:10]
        
        lines = []
        for dt, uid, _ in top_10:
            lines.append(f"<@{uid}>: {dt.strftime('%B')} {dt.day}")
            
        msg = "**Upcoming 10 Birthdays:**\n" + "\n".join(lines)
        await interaction.response.edit_message(content=msg, view=self)


class BirthdayCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot.add_view(BirthdayView())
        self.birthday_loop.start()

    def cog_unload(self):
        self.birthday_loop.cancel()

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        data = load_birthdays()
        uid_str = str(member.id)
        if uid_str in data:
            del data[uid_str]
            save_birthdays(data)
            logger.info(f"Removed birthday for left user {member.id}")

    @discord.app_commands.command(name="birthday", description="Manage and view upcoming birthdays.")
    async def birthday(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            "Manage your birthday below:",
            view=BirthdayView(),
            ephemeral=True
        )

    @tasks.loop(time=time(hour=7, minute=0, tzinfo=CHICAGO_TZ))
    async def birthday_loop(self):
        try:
            data = load_birthdays()
            if not data:
                return
                
            now = datetime.now(CHICAGO_TZ).date()
            channel = self.bot.get_channel(ANNOUNCE_CHANNEL_ID)
            if not channel:
                logger.warning(f"Birthday announcement channel {ANNOUNCE_CHANNEL_ID} not found.")
                return

            for uid_str, binfo in data.items():
                m = binfo["month"]
                d = binfo["day"]
                y = binfo["year"]
                
                is_bday = False
                if now.month == m and now.day == d:
                    is_bday = True
                elif m == 2 and d == 29 and now.month == 3 and now.day == 1:
                    # Check if current year is a leap year
                    try:
                        date(now.year, 2, 29)
                    except ValueError:
                        is_bday = True
                        
                if is_bday:
                    user_id = int(uid_str)
                    age = (now.year - y) if y else None
                    
                    if age is not None:
                        desc = f"It's <@{user_id}>'s ({age}) birthday!  :birthday::tada: Wish them a happy birthday today and tell them how much you love them! Or don’t, it’s up to you honestly."
                    else:
                        desc = f"It's <@{user_id}>'s birthday!  :birthday::tada: Wish them a happy birthday today and tell them how much you love them! Or don’t, it’s up to you honestly."
                    
                    embed = discord.Embed(description=desc, color=discord.Color.from_rgb(255, 255, 255))
                    embed.set_footer(text="To set your birthday use /birthday")
                    
                    await channel.send(embed=embed)
                    logger.info(f"Announced birthday for {user_id}")
        except Exception as e:
            logger.error(f"Error in birthday_loop: {e}", exc_info=True)

    @birthday_loop.before_loop
    async def before_birthday_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(BirthdayCog(bot))
