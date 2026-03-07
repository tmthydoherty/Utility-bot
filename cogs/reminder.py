import discord
from discord.ext import commands, tasks
from discord import app_commands
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import json
import os
import asyncio
import time
import re
import logging
import copy

# --- LOGGING SETUP ---
logger = logging.getLogger('reminders_cog')
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

# File to store reminders
REMINDERS_FILE = "reminders.json"
REMINDERS_CONFIG_FILE = "reminders_config.json"

def load_reminders_config():
    if os.path.exists(REMINDERS_CONFIG_FILE):
        try:
            with open(REMINDERS_CONFIG_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_reminders_config(config):
    with open(REMINDERS_CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)
DAY_MAPPING = {
    "mon": 0, "monday": 0,
    "tue": 1, "tuesday": 1,
    "wed": 2, "wednesday": 2,
    "thu": 3, "thursday": 3,
    "fri": 4, "friday": 4,
    "sat": 5, "saturday": 5,
    "sun": 6, "sunday": 6
}

EMBED_COLORS = {
    "Green (Default)": 0x57F287,
    "Blue": 0x3498DB,
    "Red": 0xED4245,
    "Orange": 0xE67E22,
    "Purple": 0x9B59B6,
    "Gold": 0xF1C40F,
    "Dark Grey": 0x2B2D31,
    "White": 0xFFFFFF
}


class Reminders(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.reminders = self.load_reminders()
        self.config = load_reminders_config()
        self.sticky_locks = {}
        self.sticky_timers = {}  # Tracks active sticky timers for debouncing
        self.reminder_loop.start()

    def cog_unload(self):
        self.reminder_loop.cancel()
        for task in self.sticky_timers.values():
            task.cancel()
        self.sticky_timers.clear()

    def load_reminders(self):
        if not os.path.exists(REMINDERS_FILE):
            logger.info("No reminders file found. Starting fresh.")
            return {}
        try:
            with open(REMINDERS_FILE, 'r') as f:
                data = json.load(f)

            clean_data = {}
            for k, v in data.items():
                clean_data[k] = self.sanitize_data(v)
            data = clean_data

            migrated = False
            for rid, rdata in data.items():
                if 'channel_id' in rdata:
                    if 'channel_ids' not in rdata:
                        rdata['channel_ids'] = [rdata['channel_id']]
                    del rdata['channel_id']
                    migrated = True

                if 'last_sticky_id' in rdata:
                    if 'last_sticky_ids' not in rdata and rdata.get('channel_ids'):
                        cid = str(rdata['channel_ids'][0])
                        rdata['last_sticky_ids'] = {cid: rdata['last_sticky_id']}
                    del rdata['last_sticky_id']
                    migrated = True

                if rdata.get('type') == 'scheduled' and 'schedule_text' not in rdata:
                    if 'days' in rdata and 'time' in rdata:
                        days_map_inv = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                        parts = []
                        for d in rdata['days']:
                            parts.append(f"{days_map_inv[int(d)]} {rdata['time']}")
                        rdata['schedule_text'] = ", ".join(parts)
                        rdata.pop('days', None)
                        rdata.pop('time', None)
                        migrated = True

                rdata = self.initialize_skip_data(rdata)

            if migrated:
                logger.info("Migrated reminders.")
                self.save_reminders_internal(data)

            # Timezone-aware migration
            migrated_timezone = False
            for rid, rdata in data.items():
                if rdata.get('type') == 'scheduled':
                    # Migrate schedule_text -> schedule_data
                    if 'schedule_text' in rdata and 'schedule_data' not in rdata:
                        logger.info(f"Migrating {rid} to timezone-aware format")
                        parsed = self.parse_schedule(rdata['schedule_text'])
                        est_tz = ZoneInfo('America/New_York')

                        schedule_data = {'frequency': parsed['type'], 'creation_timezone': 'America/New_York'}

                        if parsed['type'] == 'daily' and parsed['triggers']:
                            time_est = parsed['triggers'][0]['time']
                            schedule_data['time_utc'] = self._convert_time_to_utc(time_est, est_tz)

                        elif parsed['type'] in ['weekly', 'biweekly'] and parsed['triggers']:
                            days = set()
                            times = set()
                            for t in parsed['triggers']:
                                days.add(t['dow'])
                                times.add(self._convert_time_to_utc(t['time'], est_tz))
                            schedule_data['days_of_week'] = sorted(list(days))
                            schedule_data['time_utc'] = sorted(list(times))[0]

                        elif parsed['type'] == 'monthly' and parsed['triggers']:
                            t = parsed['triggers'][0]
                            schedule_data['day_of_month'] = t['dom']
                            schedule_data['time_utc'] = self._convert_time_to_utc(t['time'], est_tz)

                        rdata['schedule_data'] = schedule_data
                        migrated_timezone = True

                    # Migrate last_sent_date -> last_sent_timestamp
                    if 'last_sent_date' in rdata and 'last_sent_timestamp' not in rdata:
                        try:
                            date_str = rdata['last_sent_date']
                            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                            est_tz = ZoneInfo('America/New_York')
                            dt_with_tz = date_obj.replace(hour=12, tzinfo=est_tz)
                            rdata['last_sent_timestamp'] = int(dt_with_tz.timestamp())
                            migrated_timezone = True
                        except Exception as e:
                            logger.warning(f"Could not migrate date for {rid}: {e}")

            if migrated_timezone:
                logger.info("Migrated to timezone-aware format")
                self.save_reminders_internal(data)

            # Fix corrupted string-encoded lists (from old sanitize_data bug)
            migrated_lists = False
            for rid, rdata in data.items():
                # Fix schedule_data.days_of_week if it's a string like "[0, 1]"
                schedule_data = rdata.get('schedule_data', {})
                if isinstance(schedule_data, dict):
                    dow = schedule_data.get('days_of_week')
                    if isinstance(dow, str) and dow.startswith('['):
                        try:
                            schedule_data['days_of_week'] = json.loads(dow)
                            migrated_lists = True
                            logger.info(f"Fixed days_of_week for {rid}")
                        except json.JSONDecodeError:
                            pass

                # Fix event_schedule.days_of_week if it's a string
                event_schedule = rdata.get('event_schedule', {})
                if isinstance(event_schedule, dict):
                    dow = event_schedule.get('days_of_week')
                    if isinstance(dow, str) and dow.startswith('['):
                        try:
                            event_schedule['days_of_week'] = json.loads(dow)
                            migrated_lists = True
                            logger.info(f"Fixed event days_of_week for {rid}")
                        except json.JSONDecodeError:
                            pass

            if migrated_lists:
                logger.info("Fixed corrupted list data")
                self.save_reminders_internal(data)

            logger.info(f"Loaded {len(data)} reminders.")
            return data
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load reminders: {e}")
            return {}

    def save_reminders(self):
        self.save_reminders_internal(self.reminders)

    def save_reminders_internal(self, data_to_save):
        try:
            try:
                json.dumps(data_to_save)
            except TypeError:
                logger.warning("Corrupt memory detected. Running deep cleaning...")
                cleaned = {}
                for k, v in data_to_save.items():
                    cleaned[k] = self.sanitize_data(v)
                data_to_save = cleaned

            with open(REMINDERS_FILE, 'w') as f:
                json.dump(data_to_save, f, indent=4)
        except Exception as e:
            logger.error(f"CRITICAL SAVE ERROR: {e}")

    @staticmethod
    def sanitize_data(data):
        """Recursively ensures data is primitive types only (JSON-serializable)."""
        if data is None:
            return None
        if isinstance(data, (str, int, float, bool)):
            return data
        if hasattr(data, 'to_component_dict') or 'discord.ui' in str(type(data)):
            return str(data)
        if isinstance(data, list):
            return [Reminders.sanitize_data(item) for item in data]
        if isinstance(data, dict):
            clean = {}
            for k, v in data.items():
                clean[str(k)] = Reminders.sanitize_data(v)
            return clean
        # Fallback for unknown types
        return str(data)

    def initialize_skip_data(self, data):
        if 'skipped_dates' not in data:
            data['skipped_dates'] = []
        if 'skip_next' not in data:
            data['skip_next'] = 0
        return data

    def check_reminder_permissions(self, interaction: discord.Interaction) -> bool:
        """Check if user has permission to manage reminders."""
        if interaction.user.guild_permissions.manage_guild:
            return True

        guild_id = str(interaction.guild.id)
        config = self.config.get(guild_id, {})

        # Convert admin role IDs to set of ints for reliable comparison
        # (handles potential type mismatches from JSON serialization)
        try:
            admin_role_ids = {int(rid) for rid in config.get('admin_role_ids', [])}
        except (ValueError, TypeError):
            admin_role_ids = set()

        user_role_ids = {role.id for role in interaction.user.roles}
        return bool(admin_role_ids & user_role_ids)

    def get_guild_config(self, guild_id: int) -> dict:
        """Get guild configuration, creating if doesn't exist."""
        gid = str(guild_id)
        if gid not in self.config:
            self.config[gid] = {'admin_role_ids': [], 'timezone': 'UTC'}
        return self.config[gid]

    def save_config(self):
        """Save configuration to file."""
        save_reminders_config(self.config)

    def get_guild_timezone(self, guild_id) -> ZoneInfo:
        """Get guild's timezone from config."""
        config = self.get_guild_config(guild_id)
        tz_str = config.get('timezone', 'UTC')
        try:
            return ZoneInfo(tz_str)
        except Exception:
            logger.warning(f"Invalid timezone {tz_str}, using UTC")
            return ZoneInfo('UTC')

    def _convert_time_to_utc(self, time_str: str, source_tz: ZoneInfo) -> str:
        """Convert HH:MM time from source timezone to UTC."""
        hour, minute = map(int, time_str.split(':'))
        now = datetime.now(source_tz)
        local_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        utc_time = local_time.astimezone(timezone.utc)
        return utc_time.strftime("%H:%M")

    def _convert_time_to_local(self, time_utc_str: str, target_tz: ZoneInfo) -> str:
        """Convert HH:MM UTC time to target timezone."""
        hour, minute = map(int, time_utc_str.split(':'))
        now_utc = datetime.now(timezone.utc)
        utc_time = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
        local_time = utc_time.astimezone(target_tz)
        return local_time.strftime("%H:%M")

    # --- PARSING ENGINE ---
    def parse_schedule(self, text):
        text = text.lower().strip()
        text = re.sub(r'weekdays\s+(\d{1,2}:\d{2})',
                      lambda m: ', '.join([f"{d} {m.group(1)}" for d in ['mon', 'tue', 'wed', 'thu', 'fri']]), text)
        text = re.sub(r'weekends\s+(\d{1,2}:\d{2})',
                      lambda m: ', '.join([f"{d} {m.group(1)}" for d in ['sat', 'sun']]), text)

        result = {'type': 'weekly', 'triggers': []}

        if text.startswith("biweekly"):
            result['type'] = 'biweekly'
            text = text.replace("biweekly", "").strip()
        elif text.startswith("monthly"):
            result['type'] = 'monthly'
            text = text.replace("monthly", "").strip()
        elif text.startswith("daily") or text.startswith("everyday"):
            result['type'] = 'daily'
            text = text.replace("daily", "").replace("everyday", "").strip()

        for seg in [s.strip() for s in text.split(',')]:
            parts = seg.split()
            if result['type'] == 'daily':
                try:
                    result['triggers'].append({'time': datetime.strptime(seg, "%H:%M").strftime("%H:%M")})
                except ValueError:
                    pass
                continue

            if len(parts) < 2:
                continue
            time_part, day_part = parts[-1], " ".join(parts[:-1])

            try:
                time_fmt = datetime.strptime(time_part, "%H:%M").strftime("%H:%M")
            except ValueError:
                continue

            if result['type'] == 'monthly':
                digits = re.findall(r'\d+', day_part)
                if digits and 1 <= int(digits[0]) <= 31:
                    result['triggers'].append({'dom': int(digits[0]), 'time': time_fmt})
            else:
                clean_day = day_part.replace(".", "")
                if clean_day in DAY_MAPPING:
                    result['triggers'].append({'dow': DAY_MAPPING[clean_day], 'time': time_fmt})

        return result

    def get_next_fire_time(self, data) -> str:
        """Calculate next fire time (timezone-aware)."""
        if data.get('type') != 'scheduled':
            return "N/A"
        if not data.get('enabled', True):
            return "Disabled"

        schedule_data = data.get('schedule_data', {})
        if not schedule_data:
            return "Invalid schedule"

        now_utc = datetime.now(timezone.utc)
        frequency = schedule_data.get('frequency')
        time_utc_str = schedule_data.get('time_utc')

        if not time_utc_str or ':' not in str(time_utc_str):
            return "Unknown"

        try:
            hour, minute = map(int, str(time_utc_str).split(':'))
        except (ValueError, AttributeError):
            return "Invalid time format"

        # Calculate next occurrence
        if frequency == 'daily':
            next_fire = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_fire <= now_utc:
                next_fire += timedelta(days=1)

        elif frequency == 'monthly':
            try:
                dom = int(schedule_data.get('day_of_month', 1))
            except (ValueError, TypeError):
                return "Invalid day of month"
            # Try current month first
            try:
                next_fire = now_utc.replace(day=dom, hour=hour, minute=minute, second=0, microsecond=0)
                if next_fire <= now_utc:
                    raise ValueError("Already passed")
            except ValueError:
                # Try next month
                year = now_utc.year if now_utc.month < 12 else now_utc.year + 1
                month = now_utc.month + 1 if now_utc.month < 12 else 1
                for _ in range(12):  # Try up to 12 months
                    try:
                        next_fire = datetime(year, month, dom, hour, minute, tzinfo=timezone.utc)
                        break
                    except ValueError:
                        month += 1
                        if month > 12:
                            month = 1
                            year += 1
                else:
                    return "No valid date found"

        elif frequency in ['weekly', 'biweekly']:
            days_of_week = schedule_data.get('days_of_week', [])
            if not days_of_week:
                return "No days configured"

            # Ensure days are integers (could be strings from corrupted data)
            try:
                days_of_week = [int(d) for d in days_of_week]
            except (ValueError, TypeError):
                return "Invalid days configuration"

            # Find next matching day
            candidates = []
            for target_dow in days_of_week:
                days_ahead = (target_dow - now_utc.weekday()) % 7
                if days_ahead == 0:
                    candidate = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    if candidate <= now_utc:
                        days_ahead = 7
                    else:
                        candidates.append(candidate)
                        continue

                candidate = now_utc + timedelta(days=days_ahead)
                candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)
                candidates.append(candidate)

            if not candidates:
                return "Unknown"

            next_fire = min(candidates)

            # For biweekly, check if we need to skip a week
            if frequency == 'biweekly':
                last_fire = schedule_data.get('last_biweekly_fire')
                if last_fire:
                    last_date = datetime.fromisoformat(last_fire).date()
                    if (next_fire.date() - last_date).days < 14:
                        next_fire += timedelta(weeks=1)
        else:
            return "Unknown frequency"

        # Return Discord timestamp
        return f"<t:{int(next_fire.timestamp())}:F> (<t:{int(next_fire.timestamp())}:R>)"

    def check_schedule(self, schedule_data, last_sent_timestamp, guild_id):
        """Check if reminder should fire now (timezone-aware)."""
        now_utc = datetime.now(timezone.utc)
        current_time_utc = now_utc.strftime("%H:%M")

        if last_sent_timestamp:
            try:
                # Ensure timestamp is an integer
                ts = int(last_sent_timestamp) if isinstance(last_sent_timestamp, str) else last_sent_timestamp
                last_sent = datetime.fromtimestamp(ts, tz=timezone.utc)
                if last_sent.date() == now_utc.date():
                    return False
            except (ValueError, TypeError, OSError):
                pass  # Invalid timestamp, continue checking

        frequency = schedule_data.get('frequency')
        time_utc = schedule_data.get('time_utc')

        if not frequency or not time_utc or current_time_utc != time_utc:
            return False

        if frequency == 'daily':
            return True
        elif frequency == 'monthly':
            try:
                dom = int(schedule_data.get('day_of_month', 0))
                return now_utc.day == dom
            except (ValueError, TypeError):
                return False
        elif frequency in ['weekly', 'biweekly']:
            days_of_week = schedule_data.get('days_of_week', [])
            # Ensure days are integers for comparison
            try:
                days_of_week = [int(d) for d in days_of_week]
            except (ValueError, TypeError):
                return False
            if now_utc.weekday() not in days_of_week:
                return False
            if frequency == 'weekly':
                return True
            else:  # biweekly
                last_fire = schedule_data.get('last_biweekly_fire')
                if not last_fire:
                    return True
                last_date = datetime.fromisoformat(last_fire).date()
                return (now_utc.date() - last_date).days >= 14
        return False

    # --- MAIN LOOP ---
    @tasks.loop(minutes=1)
    async def reminder_loop(self):
        for rid, data in list(self.reminders.items()):
            if not data.get('enabled', True) or data.get('type') == 'sticky':
                continue

            schedule_data = data.get('schedule_data', {})
            guild_id = data.get('guild_id')
            if not schedule_data or not guild_id:
                continue

            if self.check_schedule(schedule_data, data.get('last_sent_timestamp'), guild_id):
                # Ensure skip_next is an integer
                skip_next = data.get('skip_next', 0)
                try:
                    skip_next = int(skip_next) if isinstance(skip_next, str) else (skip_next or 0)
                except (ValueError, TypeError):
                    skip_next = 0

                if skip_next > 0:
                    self.reminders[rid]['skip_next'] = skip_next - 1
                    logger.info(f"Skipped {data['name']}")
                    self.save_reminders()
                    continue

                if await self.send_reminder(data):
                    now_utc = datetime.now(timezone.utc)
                    self.reminders[rid]['last_sent_timestamp'] = int(now_utc.timestamp())
                    if schedule_data.get('frequency') == 'biweekly':
                        schedule_data['last_biweekly_fire'] = now_utc.date().isoformat()
                    self.save_reminders()

    @reminder_loop.before_loop
    async def before_loop(self):
        await self.bot.wait_until_ready()

    async def send_reminder(self, data):
        cids = data.get('channel_ids', [])
        if not cids:
            return False
        embed, view, content = self.build_embed(data), self.build_view(data), self.build_content(data)
        success = False
        for cid in cids:
            try:
                ch = self.bot.get_channel(cid)
                if not ch:
                    try:
                        ch = await self.bot.fetch_channel(cid)
                    except discord.NotFound:
                        logger.warning(f"Channel {cid} not found, skipping")
                        continue
                    except discord.Forbidden:
                        logger.warning(f"No access to channel {cid}, skipping")
                        continue

                if ch:
                    # Permission checks
                    perms = ch.permissions_for(ch.guild.me)
                    if not perms.send_messages or not perms.embed_links:
                        logger.warning(f"Missing send_messages or embed_links permission in {ch.name}")
                        continue
                    if view and not perms.manage_roles:
                        logger.warning(f"Missing manage_roles permission in {ch.name} - button may not work")

                    await ch.send(content=content, embed=embed, view=view)
                    success = True
            except discord.Forbidden:
                logger.error(f"Forbidden to send in channel {cid}")
            except Exception as e:
                logger.error(f"Failed to send scheduled reminder to {cid}: {e}")
        return success

    # --- BUILDERS ---
    def get_next_event_time(self, event_schedule) -> int:
        """Calculate the next occurrence of a recurring event. Returns Unix timestamp."""
        if not event_schedule:
            return None

        now_utc = datetime.now(timezone.utc)
        frequency = event_schedule.get('frequency')
        time_utc_str = event_schedule.get('time_utc')

        if not time_utc_str or ':' not in str(time_utc_str):
            return None

        try:
            hour, minute = map(int, str(time_utc_str).split(':'))
        except (ValueError, AttributeError):
            return None

        if frequency == 'monthly':
            try:
                dom = int(event_schedule.get('day_of_month', 1))
            except (ValueError, TypeError):
                return None
            try:
                next_event = now_utc.replace(day=dom, hour=hour, minute=minute, second=0, microsecond=0)
                if next_event <= now_utc:
                    raise ValueError("Already passed")
            except ValueError:
                year = now_utc.year if now_utc.month < 12 else now_utc.year + 1
                month = now_utc.month + 1 if now_utc.month < 12 else 1
                for _ in range(12):
                    try:
                        next_event = datetime(year, month, dom, hour, minute, tzinfo=timezone.utc)
                        break
                    except ValueError:
                        month += 1
                        if month > 12:
                            month = 1
                            year += 1
                else:
                    return None

        elif frequency in ['weekly', 'biweekly']:
            days_of_week = event_schedule.get('days_of_week', [])
            if not days_of_week:
                return None

            # Ensure days are integers (could be strings from corrupted data)
            try:
                days_of_week = [int(d) for d in days_of_week]
            except (ValueError, TypeError):
                return None

            candidates = []
            for target_dow in days_of_week:
                days_ahead = (target_dow - now_utc.weekday()) % 7
                if days_ahead == 0:
                    candidate = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    if candidate <= now_utc:
                        days_ahead = 7
                    else:
                        candidates.append(candidate)
                        continue

                candidate = now_utc + timedelta(days=days_ahead)
                candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)
                candidates.append(candidate)

            if not candidates:
                return None

            next_event = min(candidates)

            if frequency == 'biweekly':
                last_fire = event_schedule.get('last_biweekly_fire')
                if last_fire:
                    last_date = datetime.fromisoformat(last_fire).date()
                    if (next_event.date() - last_date).days < 14:
                        next_event += timedelta(weeks=1)
        else:
            return None

        return int(next_event.timestamp())

    def build_embed(self, data):
        desc = data.get('message', "")

        # Add recurring event schedule if present (new format)
        try:
            if data.get('event_schedule'):
                event_schedule = data['event_schedule']
                next_event_ts = self.get_next_event_time(event_schedule)
                if next_event_ts:
                    desc += f"\n\n<t:{next_event_ts}:F>\n(<t:{next_event_ts}:R>)"
            # Legacy: single event timestamp (backwards compatibility)
            elif data.get('event_timestamp_utc'):
                event_ts = data['event_timestamp_utc']
                desc += f"\n\n<t:{event_ts}:F>\n(<t:{event_ts}:R>)"
        except Exception as e:
            logger.warning(f"Error building event timestamp: {e}")

        # Add current timestamp if enabled
        if data.get('use_timestamp'):
            desc += f"\n\n*Posted:* <t:{int(time.time())}:F>"

        title = data.get('title_text') or ("Sticky Message" if data.get('type') == 'sticky' else None)
        embed = discord.Embed(title=title, description=desc, color=discord.Color(data.get('color') or 0x57F287))
        if data.get('image_url'):
            embed.set_image(url=data.get('image_url'))
        return embed

    def build_content(self, data):
        return f"<@&{data['ping_role_id']}>" if data.get('ping_role_id') else ""

    def build_view(self, data):
        reaction_role = data.get('reaction_role')
        # Validate reaction_role has required fields before building view
        if reaction_role and reaction_role.get('role_id'):
            return ReactionRoleView(reaction_role)
        return None

    # --- STICKY LOGIC ---
    @commands.Cog.listener()
    async def on_message(self, message):
        # FIX: Only ignore OUR OWN bot messages, not other bots
        # This allows other bots to trigger sticky re-send
        if message.author.id == self.bot.user.id or not message.guild:
            return

        # Check all sticky reminders for this channel
        for rid, data in self.reminders.items():
            if not data.get('enabled', True) or data.get('type') != 'sticky':
                continue
            
            channel_ids = data.get('channel_ids', [])
            if message.channel.id not in channel_ids:
                continue

            # Check if the message is our own sticky (avoid re-triggering on our sticky)
            last_sticky_ids = data.get('last_sticky_ids', {})
            last_sticky_id = last_sticky_ids.get(str(message.channel.id))
            if last_sticky_id and message.id == last_sticky_id:
                continue

            timer_key = (message.channel.id, rid)
            if timer_key in self.sticky_timers:
                self.sticky_timers[timer_key].cancel()

            # Use a small delay (1s) to batch rapid messages
            task = asyncio.create_task(self._delayed_sticky_send(message.channel, rid, timer_key))
            self.sticky_timers[timer_key] = task
            # Don't break - process all stickies for this channel

    async def _delayed_sticky_send(self, channel, rid, timer_key):
        try:
            await asyncio.sleep(1.0)

            # Verify we're still the active task
            if timer_key not in self.sticky_timers:
                return
            if self.sticky_timers[timer_key] != asyncio.current_task():
                return  # Superseded by another task

            await self.send_sticky(channel, rid)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in sticky delay wrapper: {e}")
        finally:
            # Only cleanup if still active
            if timer_key in self.sticky_timers and self.sticky_timers[timer_key] == asyncio.current_task():
                del self.sticky_timers[timer_key]

    async def send_sticky(self, channel, rid):
        if rid not in self.sticky_locks:
            self.sticky_locks[rid] = asyncio.Lock()

        # 1. PERMISSION CHECK: Fail fast if we can't send
        try:
            perms = channel.permissions_for(channel.guild.me)
        except AttributeError:
            logger.warning(f"Could not check permissions for channel (may be deleted)")
            return

        if not perms.send_messages:
            logger.warning(f"Missing 'Send Messages' permission in {channel.name}")
            return
        if not perms.embed_links:
            logger.warning(f"Missing 'Embed Links' permission in {channel.name}")
            return

        async with self.sticky_locks[rid]:
            data = self.reminders.get(rid)
            if not data:
                return

            sticky_map = data.get('last_sticky_ids', {})
            old_msg_id = sticky_map.get(str(channel.id))

            # 2. DELETE OLD with TIMEOUT
            if old_msg_id:
                try:
                    old_msg = await asyncio.wait_for(channel.fetch_message(old_msg_id), timeout=5.0)
                    await old_msg.delete()
                except discord.NotFound:
                    pass  # Already deleted
                except asyncio.TimeoutError:
                    logger.warning(f"Timed out fetching old sticky in {channel.name}, proceeding to send new.")
                except discord.Forbidden:
                    logger.warning(f"Cannot delete old sticky in {channel.name} (Missing Manage Messages?)")
                except Exception as e:
                    logger.debug(f"Non-critical delete error: {e}")

            # 3. SEND NEW with TIMEOUT
            try:
                msg = await asyncio.wait_for(
                    channel.send(
                        content=self.build_content(data),
                        embed=self.build_embed(data),
                        view=self.build_view(data)
                    ),
                    timeout=10.0
                )

                sticky_map[str(channel.id)] = msg.id
                data['last_sticky_ids'] = sticky_map
                self.save_reminders()

            except asyncio.TimeoutError:
                logger.error(f"Timed out sending new sticky to {channel.name}")
            except discord.Forbidden:
                logger.error(f"Forbidden to send in {channel.name}")
            except discord.HTTPException as e:
                logger.error(f"HTTP Error sending sticky: {e} (Check embed content?)")
            except Exception as e:
                logger.error(f"Unexpected error sending sticky: {e}")

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type == discord.InteractionType.component:
            custom_id = str(interaction.data.get('custom_id', ''))
            if custom_id.startswith("remind_role:"):
                if not interaction.guild:
                    return await interaction.response.send_message("This button only works in servers.", ephemeral=True)
                try:
                    role_id = int(custom_id.split(":")[1])
                    role = interaction.guild.get_role(role_id)
                    if not role:
                        return await interaction.response.send_message("Role not found.", ephemeral=True)

                    # Check if role is managed (bot role, integration, etc.)
                    if role.managed:
                        return await interaction.response.send_message(
                            f"{role.mention} is managed by an integration and cannot be manually assigned.",
                            ephemeral=True
                        )

                    # Check bot permissions
                    bot_perms = interaction.guild.me.guild_permissions
                    if not bot_perms.manage_roles:
                        return await interaction.response.send_message(
                            "I don't have the 'Manage Roles' permission in this server.",
                            ephemeral=True
                        )

                    # Check role hierarchy
                    bot_top_role = interaction.guild.me.top_role
                    if role >= bot_top_role:
                        return await interaction.response.send_message(
                            f"I can't manage {role.mention} because it's at or above my highest role ({bot_top_role.mention}). "
                            f"Move my role higher in Server Settings → Roles.",
                            ephemeral=True
                        )

                    if role in interaction.user.roles:
                        await interaction.user.remove_roles(role)
                        await interaction.response.send_message(f"Removed {role.mention}", ephemeral=True)
                    else:
                        await interaction.user.add_roles(role)
                        await interaction.response.send_message(f"Added {role.mention}", ephemeral=True)
                except discord.Forbidden as e:
                    logger.error(f"Forbidden error managing role: {e}")
                    await interaction.response.send_message(
                        f"Permission denied. Error: {e}",
                        ephemeral=True
                    )
                except ValueError:
                    await interaction.response.send_message("Invalid role ID.", ephemeral=True)
                except Exception as e:
                    logger.error(f"Role toggle error: {e}")
                    if not interaction.response.is_done():
                        await interaction.response.send_message("Error managing role.", ephemeral=True)

    @app_commands.command(name="reminders_panel", description="Manage Server Reminders")
    @app_commands.default_permissions(administrator=True)
    async def reminders_panel(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("This command only works in servers.", ephemeral=True)

        if not self.check_reminder_permissions(interaction):
            return await interaction.response.send_message(
                "You need 'Manage Server' permission or a configured admin role to use this command.",
                ephemeral=True
            )

        guild_reminders = {k: v for k, v in self.reminders.items() if v.get('guild_id') == interaction.guild.id}
        await interaction.response.send_message(
            embed=build_panel_embed(guild_reminders),
            view=ReminderPanelView(self, interaction.guild),
            ephemeral=True
        )

    async def save_final(self, interaction, data):
        data = self.sanitize_data(data)
        rid = data.get('editing_key') or f"{data['name']}_{interaction.id}"

        if data['type'] == 'sticky' and data.get('editing_key'):
            old = self.reminders.get(rid)
            if old:
                old_c, new_c = set(old.get('channel_ids', [])), set(data.get('channel_ids', []))
                for cid in (old_c - new_c):
                    sid = old.get('last_sticky_ids', {}).get(str(cid))
                    if sid:
                        ch = interaction.guild.get_channel(cid)
                        if ch:
                            try:
                                old_msg = await ch.fetch_message(sid)
                                await old_msg.delete()
                            except Exception:
                                pass

        if data['type'] == 'scheduled':
            old = self.reminders.get(rid)
            if old:
                data['skipped_dates'] = old.get('skipped_dates', [])
                data['skip_next'] = old.get('skip_next', 0)
                if old.get('schedule_text') != data.get('schedule_text'):
                    data.pop('last_sent_date', None)

        self.reminders[rid] = data
        self.save_reminders()

        guild_reminders = {k: v for k, v in self.reminders.items() if v.get('guild_id') == interaction.guild.id}
        count = len(data.get('channel_ids', []))
        embed = build_panel_embed(guild_reminders)
        embed.set_footer(text=f"{data['type'].title()} '{data['name']}' saved for {count} channel(s).")

        if interaction.response.is_done():
            await interaction.edit_original_response(content=None, embed=embed, view=ReminderPanelView(self, interaction.guild))
        else:
            await interaction.response.edit_message(content=None, embed=embed, view=ReminderPanelView(self, interaction.guild))

    def cleanup_sticky_timers_for_reminder(self, rid):
        """Cancel and remove any pending sticky timers for a reminder."""
        keys_to_remove = [key for key in self.sticky_timers if key[1] == rid]
        for key in keys_to_remove:
            self.sticky_timers[key].cancel()
            del self.sticky_timers[key]


# --- PANEL HELPERS ---

def _format_schedule_summary(data):
    """Format schedule_data into a readable string."""
    schedule_data = data.get('schedule_data') or {}
    freq = schedule_data.get('frequency', 'unknown')
    time_utc = schedule_data.get('time_utc', '')
    DAY_ABBR = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    if freq == 'daily':
        return f"Daily {time_utc} UTC"
    elif freq in ['weekly', 'biweekly']:
        days = schedule_data.get('days_of_week') or []
        try:
            day_str = '/'.join(DAY_ABBR[int(d)] for d in sorted(days)) if days else '?'
        except (ValueError, TypeError, IndexError):
            day_str = '?'
        label = 'Bi-weekly' if freq == 'biweekly' else 'Weekly'
        return f"{label} {day_str} {time_utc} UTC"
    elif freq == 'monthly':
        dom = schedule_data.get('day_of_month', '?')
        return f"Monthly Day {dom} {time_utc} UTC"
    return freq.title()


def build_panel_embed(reminders):
    """Build the main reminder management panel embed."""
    scheduled = [(rid, d) for rid, d in reminders.items() if d.get('type') == 'scheduled']
    stickies = [(rid, d) for rid, d in reminders.items() if d.get('type') == 'sticky']
    lines = []
    if scheduled:
        lines.append("**Scheduled Reminders**")
        for rid, d in scheduled:
            status = "Enabled" if d.get('enabled', True) else "Disabled"
            sched = _format_schedule_summary(d)
            lines.append(f"\u2003{d.get('name', rid)} \u2014 {sched} \u2014 {status}")
    if stickies:
        if lines:
            lines.append("")
        lines.append("**Sticky Messages**")
        for rid, d in stickies:
            status = "Active" if d.get('enabled', True) else "Inactive"
            cids = d.get('channel_ids', [])
            ch_str = f"{len(cids)} channel(s)" if cids else "No channels"
            lines.append(f"\u2003{d.get('name', rid)} \u2014 {ch_str} \u2014 {status}")
    if not reminders:
        lines.append("No reminders configured. Use the buttons below to create one.")
    return discord.Embed(title="Reminder Management", description="\n".join(lines), color=0x2B2D31)


def build_detail_embed(cog, rid, data):
    """Build detail embed for a single reminder."""
    rtype = data.get('type', 'unknown').title()
    status = "Enabled" if data.get('enabled', True) else "Disabled"
    lines = [f"**Type:** {rtype}", f"**Status:** {status}"]
    if data.get('type') == 'scheduled':
        lines.append(f"**Schedule:** {_format_schedule_summary(data)}")
        next_fire = cog.get_next_fire_time(data)
        lines.append(f"**Next:** {next_fire}")
        skip_next = data.get('skip_next', 0)
        skipped_dates = data.get('skipped_dates', [])
        if skip_next:
            lines.append(f"**Skip Count:** {skip_next} occurrence(s)")
        if skipped_dates:
            dates_str = ", ".join(skipped_dates[:3])
            if len(skipped_dates) > 3:
                dates_str += f" (+{len(skipped_dates) - 3} more)"
            lines.append(f"**Skipped Dates:** {dates_str}")
    cids = data.get('channel_ids', [])
    lines.append(f"**Channels:** {len(cids)} configured")
    if data.get('ping_role_id'):
        lines.append(f"**Ping Role:** <@&{data['ping_role_id']}>")
    if data.get('event_schedule'):
        es = data['event_schedule']
        lines.append(f"**Event Schedule:** {es.get('frequency','').title()} {es.get('time_utc','')} UTC")
    return discord.Embed(
        title=data.get('name', rid),
        description="\n".join(lines),
        color=discord.Color(data.get('color') or 0x57F287)
    )


def build_skip_embed(data):
    """Build embed for the skip view."""
    skip_next = data.get('skip_next', 0)
    skipped_dates = data.get('skipped_dates', [])
    lines = [
        f"**Skip Count:** {skip_next} upcoming occurrence(s) will be skipped",
        f"**Specific Dates:** {len(skipped_dates)} date(s) skipped",
    ]
    if skipped_dates:
        lines.append("**Skipped Dates:**")
        for d in skipped_dates[:5]:
            lines.append(f"  - {d}")
        if len(skipped_dates) > 5:
            lines.append(f"  ... (+{len(skipped_dates) - 5} more)")
    return discord.Embed(
        title=f"Skip Settings \u2014 {data.get('name', '')}",
        description="\n".join(lines),
        color=0xE67E22
    )


def get_next_occurrences(data, n=8):
    """Get next N occurrence datetimes for a scheduled reminder."""
    schedule_data = data.get('schedule_data') or {}
    frequency = schedule_data.get('frequency')
    time_utc_str = schedule_data.get('time_utc')
    if not frequency or not time_utc_str:
        return []
    try:
        hour, minute = map(int, str(time_utc_str).split(':'))
    except (ValueError, AttributeError):
        return []
    now_utc = datetime.now(timezone.utc)
    occurrences = []
    if frequency == 'daily':
        next_dt = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_dt <= now_utc:
            next_dt += timedelta(days=1)
        for i in range(n):
            occurrences.append(next_dt + timedelta(days=i))
    elif frequency in ['weekly', 'biweekly']:
        try:
            days_of_week = [int(d) for d in schedule_data.get('days_of_week') or []]
        except (ValueError, TypeError):
            return []
        if not days_of_week:
            return []
        max_days = n * (14 if frequency == 'biweekly' else 7) + 14
        for delta in range(max_days):
            candidate = (now_utc + timedelta(days=delta)).replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )
            if candidate <= now_utc:
                continue
            if candidate.weekday() in days_of_week:
                occurrences.append(candidate)
                if len(occurrences) >= n:
                    break
    elif frequency == 'monthly':
        try:
            dom = int(schedule_data.get('day_of_month', 1))
        except (ValueError, TypeError):
            return []
        year, month = now_utc.year, now_utc.month
        for _ in range(n * 2 + 4):
            try:
                candidate = datetime(year, month, dom, hour, minute, tzinfo=timezone.utc)
                if candidate > now_utc:
                    occurrences.append(candidate)
                    if len(occurrences) >= n:
                        break
            except ValueError:
                pass
            month += 1
            if month > 12:
                month = 1
                year += 1
    return occurrences[:n]


# --- BASE VIEW ---

class BaseView(discord.ui.View):
    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        logger.error(f"View Error: {error}", exc_info=True)
        try:
            if interaction.response.is_done():
                await interaction.edit_original_response(content=f"Error: {error}", embed=None, view=None)
            else:
                await interaction.response.edit_message(content=f"Error: {error}", embed=None, view=None)
        except Exception:
            pass

    async def on_timeout(self):
        for item in self.children:
            if hasattr(item, 'disabled'):
                item.disabled = True


# --- MAIN PANEL VIEW ---

class ReminderPanelView(BaseView):
    def __init__(self, cog, guild):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild = guild

        guild_reminders = {k: v for k, v in cog.reminders.items() if v.get('guild_id') == guild.id}

        # Row 0: reminder select (up to 25)
        if guild_reminders:
            options = []
            for rid, d in list(guild_reminders.items())[:25]:
                type_label = "Sticky" if d.get('type') == 'sticky' else "Sched"
                label = f"[{type_label}] {d.get('name', rid)}"[:100]
                if d.get('type') == 'scheduled':
                    desc = _format_schedule_summary(d)[:100]
                else:
                    desc = f"{len(d.get('channel_ids', []))} channel(s)"
                options.append(discord.SelectOption(label=label, value=rid, description=desc))
            self.reminder_select = discord.ui.Select(
                placeholder="Select a reminder to manage...",
                options=options,
                row=0
            )
            self.reminder_select.callback = self.on_select_reminder
            self.add_item(self.reminder_select)

        # Row 1: action buttons
        new_sched_btn = discord.ui.Button(label="New Scheduled", style=discord.ButtonStyle.primary, row=1)
        new_sched_btn.callback = self.on_new_scheduled
        self.add_item(new_sched_btn)

        new_sticky_btn = discord.ui.Button(label="New Sticky", style=discord.ButtonStyle.primary, row=1)
        new_sticky_btn.callback = self.on_new_sticky
        self.add_item(new_sticky_btn)

        settings_btn = discord.ui.Button(label="Settings", style=discord.ButtonStyle.secondary, row=1)
        settings_btn.callback = self.on_settings
        self.add_item(settings_btn)

    async def on_select_reminder(self, interaction: discord.Interaction):
        rid = self.reminder_select.values[0]
        data = self.cog.reminders.get(rid)
        if not data:
            guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
            return await interaction.response.edit_message(
                content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
            )
        await interaction.response.edit_message(
            content=None,
            embed=build_detail_embed(self.cog, rid, data),
            view=ReminderDetailView(self.cog, rid, self.guild)
        )

    async def on_new_scheduled(self, interaction: discord.Interaction):
        await interaction.response.send_modal(ReminderModal(self.cog, 'scheduled', guild=self.guild))

    async def on_new_sticky(self, interaction: discord.Interaction):
        await interaction.response.send_modal(ReminderModal(self.cog, 'sticky', guild=self.guild))

    async def on_settings(self, interaction: discord.Interaction):
        config = self.cog.get_guild_config(self.guild.id)
        embed = discord.Embed(
            title="Reminder Settings",
            description=(
                f"**Timezone:** {config.get('timezone', 'UTC')}\n"
                f"**Admin Roles:** {len(config.get('admin_role_ids', []))} configured"
            ),
            color=0x2B2D31
        )
        await interaction.response.edit_message(embed=embed, view=ReminderSettingsView(self.cog, self.guild))


# --- REMINDER DETAIL VIEW ---

class ReminderDetailView(BaseView):
    def __init__(self, cog, rid, guild):
        super().__init__(timeout=300)
        self.cog = cog
        self.rid = rid
        self.guild = guild

        data = cog.reminders.get(rid, {})
        options = [
            discord.SelectOption(label="Preview", value="preview", description="Preview how the reminder looks"),
            discord.SelectOption(label="Test", value="test", description="Send an immediate test to all channels"),
            discord.SelectOption(label="Toggle", value="toggle", description="Enable or disable this reminder"),
            discord.SelectOption(label="Edit Content", value="edit_content", description="Edit name, title, message, image"),
            discord.SelectOption(label="Edit Channels", value="edit_channels", description="Change which channels this posts to"),
            discord.SelectOption(label="Duplicate", value="duplicate", description="Create a copy of this reminder"),
            discord.SelectOption(label="Delete", value="delete", description="Permanently delete this reminder"),
        ]
        if data.get('type') == 'scheduled':
            options.insert(4, discord.SelectOption(label="Edit Schedule", value="edit_schedule", description="Change frequency, days, time"))
            options.insert(6, discord.SelectOption(label="Skip", value="skip", description="Skip upcoming occurrences"))

        self.action_select = discord.ui.Select(placeholder="Select an action...", options=options, row=0)
        self.action_select.callback = self.on_action
        self.add_item(self.action_select)

        back_btn = discord.ui.Button(label="Back to List", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.on_back
        self.add_item(back_btn)

    async def on_back(self, interaction: discord.Interaction):
        guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
        await interaction.response.edit_message(
            content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
        )

    async def on_action(self, interaction: discord.Interaction):
        action = self.action_select.values[0]
        d = self.cog.reminders.get(self.rid)

        def _back_to_panel():
            guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
            return build_panel_embed(guild_reminders), ReminderPanelView(self.cog, self.guild)

        if not d and action != "delete":
            embed, view = _back_to_panel()
            return await interaction.response.edit_message(content=None, embed=embed, view=view)

        if action == "preview":
            try:
                embed = self.cog.build_embed(d)
                preview_view = self.cog.build_view(d)
                content = self.cog.build_content(d) or None
                await interaction.response.edit_message(
                    content=content, embed=embed,
                    view=PreviewReturnView(self.cog, self.rid, self.guild, preview_view)
                )
            except Exception as e:
                logger.error(f"Preview error: {e}")
                await interaction.response.edit_message(
                    content=f"Error generating preview: {e}", embed=None,
                    view=ReminderDetailView(self.cog, self.rid, self.guild)
                )

        elif action == "test":
            await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(title="Confirm Test", description="Send a test message to all configured channels?", color=0x2B2D31),
                view=TestConfirmView(self.cog, self.rid, self.guild)
            )

        elif action == "toggle":
            d['enabled'] = not d.get('enabled', True)
            self.cog.save_reminders()
            await interaction.response.edit_message(
                content=None,
                embed=build_detail_embed(self.cog, self.rid, d),
                view=ReminderDetailView(self.cog, self.rid, self.guild)
            )

        elif action == "edit_content":
            editing_data = self.cog.sanitize_data(d)
            await interaction.response.send_modal(
                ReminderModal(self.cog, d['type'], editing_data, self.rid, guild=self.guild)
            )

        elif action == "edit_schedule":
            if d.get('type') != 'scheduled':
                return await interaction.response.edit_message(
                    content="Only available for scheduled reminders.", embed=None,
                    view=ReminderDetailView(self.cog, self.rid, self.guild)
                )
            editing_data = self.cog.sanitize_data(d)
            await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(title="Edit Schedule", description="Configure when this reminder fires.", color=0x2B2D31),
                view=ScheduleView(self.cog, editing_data, self.guild, editing_rid=self.rid)
            )

        elif action == "edit_channels":
            editing_data = self.cog.sanitize_data(d)
            await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(title="Edit Channels", description="Select which channels this reminder posts to.", color=0x2B2D31),
                view=EditChannelsView(self.cog, self.rid, editing_data, self.guild)
            )

        elif action == "skip":
            if d.get('type') != 'scheduled':
                return await interaction.response.edit_message(
                    content="Only available for scheduled reminders.", embed=None,
                    view=ReminderDetailView(self.cog, self.rid, self.guild)
                )
            await interaction.response.edit_message(
                content=None, embed=build_skip_embed(d),
                view=SkipView(self.cog, self.rid, self.guild)
            )

        elif action == "duplicate":
            clean_source = self.cog.sanitize_data(d)
            new_d = copy.deepcopy(clean_source)
            new_d.update({
                'name': f"{d['name']} (Copy)", 'editing_key': None,
                'last_sent_timestamp': None, 'last_sticky_ids': {},
                'skip_next': 0, 'skipped_dates': []
            })
            await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(title="Configure Duplicate", description="Set up the copied reminder.", color=0x2B2D31),
                view=ReminderConfigView(self.cog, new_d, self.guild)
            )

        elif action == "delete":
            data_name = d['name'] if d else self.rid
            await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(
                    title="Confirm Delete",
                    description=f"Are you sure you want to delete **{data_name}**? This cannot be undone.",
                    color=0xED4245
                ),
                view=DeleteConfirmView(self.cog, self.rid, self.guild)
            )


# --- PREVIEW RETURN VIEW ---

class PreviewReturnView(BaseView):
    def __init__(self, cog, rid, guild, reaction_view=None):
        super().__init__(timeout=180)
        self.cog = cog
        self.rid = rid
        self.guild = guild
        if reaction_view:
            for item in reaction_view.children:
                self.add_item(item)
        back_btn = discord.ui.Button(label="Back to Detail", style=discord.ButtonStyle.secondary, row=4)
        back_btn.callback = self.on_back
        self.add_item(back_btn)

    async def on_back(self, interaction: discord.Interaction):
        d = self.cog.reminders.get(self.rid)
        if not d:
            guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
            return await interaction.response.edit_message(
                content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
            )
        await interaction.response.edit_message(
            content=None, embed=build_detail_embed(self.cog, self.rid, d),
            view=ReminderDetailView(self.cog, self.rid, self.guild)
        )


# --- TEST CONFIRM VIEW ---

class TestConfirmView(BaseView):
    def __init__(self, cog, rid, guild):
        super().__init__(timeout=30)
        self.cog = cog
        self.rid = rid
        self.guild = guild

        confirm_btn = discord.ui.Button(label="Confirm Test", style=discord.ButtonStyle.primary)
        confirm_btn.callback = self.on_confirm
        self.add_item(confirm_btn)

        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)
        cancel_btn.callback = self.on_cancel
        self.add_item(cancel_btn)

    async def on_confirm(self, interaction: discord.Interaction):
        d = self.cog.reminders.get(self.rid)
        if not d:
            guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
            return await interaction.response.edit_message(
                content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
            )
        channel_ids = d.get('channel_ids', [])
        if not channel_ids:
            return await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(title="No Channels", description="No channels configured for this reminder.", color=0xED4245),
                view=ReminderDetailView(self.cog, self.rid, self.guild)
            )
        await interaction.response.edit_message(content="Sending test...", embed=None, view=None)
        try:
            success = await self.cog.send_reminder(d)
            d_fresh = self.cog.reminders.get(self.rid)
            if d_fresh:
                detail_embed = build_detail_embed(self.cog, self.rid, d_fresh)
                result = f"Test sent to {len(channel_ids)} channel(s)." if success else "Failed to send. Check bot permissions."
                detail_embed.set_footer(text=result)
                await interaction.edit_original_response(
                    content=None, embed=detail_embed,
                    view=ReminderDetailView(self.cog, self.rid, self.guild)
                )
        except Exception as e:
            logger.error(f"Test send error: {e}")
            await interaction.edit_original_response(
                content=None,
                embed=discord.Embed(title="Error", description=f"Error sending test: {e}", color=0xED4245),
                view=ReminderDetailView(self.cog, self.rid, self.guild)
            )

    async def on_cancel(self, interaction: discord.Interaction):
        d = self.cog.reminders.get(self.rid)
        if not d:
            guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
            return await interaction.response.edit_message(
                content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
            )
        await interaction.response.edit_message(
            content=None, embed=build_detail_embed(self.cog, self.rid, d),
            view=ReminderDetailView(self.cog, self.rid, self.guild)
        )


# --- DELETE CONFIRM VIEW ---

class DeleteConfirmView(BaseView):
    def __init__(self, cog, rid, guild):
        super().__init__(timeout=30)
        self.cog = cog
        self.rid = rid
        self.guild = guild

        confirm_btn = discord.ui.Button(label="Yes, Delete", style=discord.ButtonStyle.danger)
        confirm_btn.callback = self.on_confirm
        self.add_item(confirm_btn)

        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)
        cancel_btn.callback = self.on_cancel
        self.add_item(cancel_btn)

    async def on_confirm(self, interaction: discord.Interaction):
        d = self.cog.reminders.pop(self.rid, None)
        if d:
            self.cog.cleanup_sticky_timers_for_reminder(self.rid)
            if d.get('type') == 'sticky' and interaction.guild:
                for cid, mid in d.get('last_sticky_ids', {}).items():
                    try:
                        ch = interaction.guild.get_channel(int(cid))
                        if ch:
                            msg = await ch.fetch_message(mid)
                            await msg.delete()
                    except Exception:
                        pass
            if self.rid in self.cog.sticky_locks:
                del self.cog.sticky_locks[self.rid]
            self.cog.save_reminders()
        guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
        embed = build_panel_embed(guild_reminders)
        embed.set_footer(text="Reminder deleted.")
        await interaction.response.edit_message(
            content=None, embed=embed, view=ReminderPanelView(self.cog, self.guild)
        )

    async def on_cancel(self, interaction: discord.Interaction):
        d = self.cog.reminders.get(self.rid)
        if not d:
            guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
            return await interaction.response.edit_message(
                content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
            )
        await interaction.response.edit_message(
            content=None, embed=build_detail_embed(self.cog, self.rid, d),
            view=ReminderDetailView(self.cog, self.rid, self.guild)
        )


# --- REMINDER MODAL ---

class ReminderModal(discord.ui.Modal):
    def __init__(self, cog, r_type, data=None, rid=None, guild=None):
        super().__init__(title=f"{r_type.title()} Setup")
        self.cog = cog
        self.r_type = r_type
        self.data = data or {}
        self.rid = rid
        self.guild = guild

        self.name_field = discord.ui.TextInput(
            label="Name", default=str(self.data.get('name', '') or ''),
            required=True, max_length=100
        )
        self.title_field = discord.ui.TextInput(
            label="Title", default=str(self.data.get('title_text', '') or ''),
            required=False, max_length=256
        )
        self.msg_field = discord.ui.TextInput(
            label="Message", style=discord.TextStyle.paragraph,
            default=str(self.data.get('message', '') or '')[:3900],
            required=True, max_length=3900
        )
        self.img_field = discord.ui.TextInput(
            label="Image URL", default=str(self.data.get('image_url', '') or ''),
            required=False, max_length=500
        )
        self.add_item(self.name_field)
        self.add_item(self.title_field)
        self.add_item(self.msg_field)
        self.add_item(self.img_field)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"Modal error in {self.title}: {error}", exc_info=True)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(f"Error: {error}", ephemeral=True)
            else:
                await interaction.response.send_message(f"Error: {error}", ephemeral=True)
        except Exception:
            pass

    async def on_submit(self, interaction: discord.Interaction):
        new_data = {
            'type': self.r_type,
            'name': str(self.name_field.value),
            'title_text': str(self.title_field.value),
            'message': str(self.msg_field.value),
            'image_url': str(self.img_field.value),
            'editing_key': self.rid,
            'channel_ids': self.data.get('channel_ids', []),
            'ping_role_id': self.data.get('ping_role_id'),
            'reaction_role': self.data.get('reaction_role'),
            'color': self.data.get('color'),
            'use_timestamp': self.data.get('use_timestamp', False),
            'enabled': self.data.get('enabled', True),
            'guild_id': interaction.guild.id,
            'schedule_data': self.data.get('schedule_data'),
            'event_schedule': self.data.get('event_schedule'),
            'last_sent_timestamp': self.data.get('last_sent_timestamp'),
            'skip_next': self.data.get('skip_next', 0),
            'skipped_dates': self.data.get('skipped_dates', [])
        }

        img_url = new_data['image_url'].strip()
        if img_url and not (img_url.startswith('http://') or img_url.startswith('https://')):
            return await interaction.response.send_message(
                "Invalid URL. Must start with http:// or https://", ephemeral=True
            )

        if not self.rid:
            for d in self.cog.reminders.values():
                if (d.get('name') or '').lower() == new_data['name'].lower():
                    return await interaction.response.send_message(
                        f"A reminder named `{new_data['name']}` already exists. Please choose a different name.",
                        ephemeral=True
                    )

        guild = self.guild or interaction.guild
        if self.r_type == 'scheduled':
            await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(title="Schedule Configuration", description="Select when this reminder should fire.", color=0x2B2D31),
                view=ScheduleView(self.cog, new_data, guild, editing_rid=self.rid)
            )
        else:
            try:
                json.dumps(new_data)
            except TypeError:
                new_data = self.cog.sanitize_data(new_data)
            await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(title="Configuration", description="Select channels, roles, and appearance.", color=0x2B2D31),
                view=ReminderConfigView(self.cog, new_data, guild)
            )


# --- SCHEDULE VIEW ---

class ScheduleView(BaseView):
    def __init__(self, cog, reminder_data, guild, editing_rid=None):
        super().__init__(timeout=600)
        self.cog = cog
        self.data = reminder_data
        self.guild = guild
        self.editing_rid = editing_rid

        schedule_data = reminder_data.get('schedule_data') or {}
        self.selected_frequency = schedule_data.get('frequency', 'weekly')

        time_utc = schedule_data.get('time_utc')
        if time_utc and ':' in str(time_utc):
            try:
                guild_tz = cog.get_guild_timezone(guild.id)
                local_time = cog._convert_time_to_local(str(time_utc), guild_tz)
                h_local, m_local = local_time.split(':')
                self.selected_hour = int(h_local)
                self.selected_minute = int(m_local)
            except (ValueError, AttributeError):
                self.selected_hour = None
                self.selected_minute = None
        else:
            self.selected_hour = None
            self.selected_minute = None

        self.selected_days = list(schedule_data.get('days_of_week') or [])
        self.selected_dom = schedule_data.get('day_of_month')
        self.build_ui()

    def _summary(self):
        parts = [f"**Frequency:** {self.selected_frequency.title()}"]
        DAY_ABBR = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        if self.selected_frequency in ['weekly', 'biweekly'] and self.selected_days:
            try:
                parts.append(f"**Days:** {', '.join(DAY_ABBR[d] for d in sorted(self.selected_days))}")
            except (IndexError, TypeError):
                pass
        elif self.selected_frequency == 'monthly' and self.selected_dom:
            parts.append(f"**Day of Month:** {self.selected_dom}")
        if self.selected_hour is not None and self.selected_minute is not None:
            parts.append(f"**Time:** {self.selected_hour:02d}:{self.selected_minute:02d} (local)")
        return "\n".join(parts)

    def build_ui(self):
        self.clear_items()
        freq_select = discord.ui.Select(
            placeholder="Frequency...",
            options=[
                discord.SelectOption(label="Daily", value="daily", default=self.selected_frequency == 'daily'),
                discord.SelectOption(label="Weekly", value="weekly", default=self.selected_frequency == 'weekly'),
                discord.SelectOption(label="Biweekly", value="biweekly", default=self.selected_frequency == 'biweekly'),
                discord.SelectOption(label="Monthly", value="monthly", default=self.selected_frequency == 'monthly'),
            ],
            row=0
        )
        freq_select.callback = self.on_frequency
        self.add_item(freq_select)

        if self.selected_frequency in ['weekly', 'biweekly']:
            dow_select = discord.ui.Select(
                placeholder="Days of week...",
                options=[
                    discord.SelectOption(label="Monday", value="0", default=0 in self.selected_days),
                    discord.SelectOption(label="Tuesday", value="1", default=1 in self.selected_days),
                    discord.SelectOption(label="Wednesday", value="2", default=2 in self.selected_days),
                    discord.SelectOption(label="Thursday", value="3", default=3 in self.selected_days),
                    discord.SelectOption(label="Friday", value="4", default=4 in self.selected_days),
                    discord.SelectOption(label="Saturday", value="5", default=5 in self.selected_days),
                    discord.SelectOption(label="Sunday", value="6", default=6 in self.selected_days),
                ],
                min_values=1, max_values=7, row=1
            )
            dow_select.callback = self.on_days
            self.add_item(dow_select)
        elif self.selected_frequency == 'monthly':
            dom_select = discord.ui.Select(
                placeholder="Day of month...",
                options=[discord.SelectOption(label=f"Day {i}", value=str(i), default=self.selected_dom == i) for i in range(1, 26)],
                row=1
            )
            dom_select.callback = self.on_dom
            self.add_item(dom_select)

        hour_select = discord.ui.Select(
            placeholder="Hour (0-23)...",
            options=[discord.SelectOption(label=f"{i:02d}:00", value=str(i), default=self.selected_hour == i) for i in range(24)],
            row=2
        )
        hour_select.callback = self.on_hour
        self.add_item(hour_select)

        minute_select = discord.ui.Select(
            placeholder="Minute...",
            options=[discord.SelectOption(label=f":{i:02d}", value=str(i), default=self.selected_minute == i) for i in [0, 15, 30, 45]],
            row=3
        )
        minute_select.callback = self.on_minute
        self.add_item(minute_select)

        guild_tz = self.cog.get_guild_timezone(self.guild.id)
        info_btn = discord.ui.Button(label=f"Timezone: {guild_tz}", style=discord.ButtonStyle.secondary, disabled=True, row=4)
        self.add_item(info_btn)

        next_btn = discord.ui.Button(label="Next: Event Time", style=discord.ButtonStyle.primary, row=4)
        next_btn.callback = self.on_next
        self.add_item(next_btn)

    async def on_frequency(self, interaction: discord.Interaction):
        self.selected_frequency = interaction.data['values'][0]
        self.selected_days = []
        self.selected_dom = None
        self.build_ui()
        await interaction.response.edit_message(
            embed=discord.Embed(title="Schedule Configuration", description=self._summary(), color=0x2B2D31),
            view=self
        )

    async def on_days(self, interaction: discord.Interaction):
        self.selected_days = [int(v) for v in interaction.data['values']]
        await interaction.response.edit_message(
            embed=discord.Embed(title="Schedule Configuration", description=self._summary(), color=0x2B2D31),
            view=self
        )

    async def on_dom(self, interaction: discord.Interaction):
        self.selected_dom = int(interaction.data['values'][0])
        await interaction.response.edit_message(
            embed=discord.Embed(title="Schedule Configuration", description=self._summary(), color=0x2B2D31),
            view=self
        )

    async def on_hour(self, interaction: discord.Interaction):
        self.selected_hour = int(interaction.data['values'][0])
        await interaction.response.edit_message(
            embed=discord.Embed(title="Schedule Configuration", description=self._summary(), color=0x2B2D31),
            view=self
        )

    async def on_minute(self, interaction: discord.Interaction):
        self.selected_minute = int(interaction.data['values'][0])
        await interaction.response.edit_message(
            embed=discord.Embed(title="Schedule Configuration", description=self._summary(), color=0x2B2D31),
            view=self
        )

    async def on_next(self, interaction: discord.Interaction):
        if self.selected_hour is None or self.selected_minute is None:
            return await interaction.response.send_message("Please select both hour and minute.", ephemeral=True)
        if self.selected_frequency in ['weekly', 'biweekly'] and not self.selected_days:
            return await interaction.response.send_message("Please select at least one day of the week.", ephemeral=True)
        if self.selected_frequency == 'monthly' and self.selected_dom is None:
            return await interaction.response.send_message("Please select a day of month.", ephemeral=True)

        guild_tz = self.cog.get_guild_timezone(interaction.guild.id)
        time_local = f"{self.selected_hour:02d}:{self.selected_minute:02d}"
        time_utc = self.cog._convert_time_to_utc(time_local, guild_tz)

        schedule_data = {
            'frequency': self.selected_frequency,
            'time_utc': time_utc,
            'creation_timezone': str(guild_tz)
        }
        if self.selected_frequency in ['weekly', 'biweekly']:
            schedule_data['days_of_week'] = self.selected_days
        elif self.selected_frequency == 'monthly':
            schedule_data['day_of_month'] = self.selected_dom
        self.data['schedule_data'] = schedule_data

        await interaction.response.edit_message(
            content=None,
            embed=discord.Embed(
                title="Add Event Timestamp?",
                description="Should this reminder include a recurring event timestamp?\nExample: a reminder fires 1 hour before an event, and shows when the event actually starts.",
                color=0x2B2D31
            ),
            view=EventTimestampChoiceView(self.cog, self.data, self.guild, editing_rid=self.editing_rid)
        )


# --- EVENT TIMESTAMP CHOICE ---

class EventTimestampChoiceView(BaseView):
    def __init__(self, cog, reminder_data, guild, editing_rid=None):
        super().__init__(timeout=600)
        self.cog = cog
        self.data = reminder_data
        self.guild = guild
        self.editing_rid = editing_rid

        skip_btn = discord.ui.Button(label="No Event Time (Skip)", style=discord.ButtonStyle.secondary)
        skip_btn.callback = self.on_skip
        self.add_item(skip_btn)

        add_btn = discord.ui.Button(label="Add Event Time", style=discord.ButtonStyle.primary)
        add_btn.callback = self.on_add
        self.add_item(add_btn)

    async def on_skip(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content=None,
            embed=discord.Embed(title="Configuration", description="Select channels, roles, and appearance.", color=0x2B2D31),
            view=ReminderConfigView(self.cog, self.data, self.guild, editing_rid=self.editing_rid)
        )

    async def on_add(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content=None,
            embed=discord.Embed(
                title="Event Schedule",
                description="Configure when the actual event occurs.\nExample: Movie Night happens every Friday at 8pm.",
                color=0x2B2D31
            ),
            view=EventScheduleConfigView(self.cog, self.data, self.guild, editing_rid=self.editing_rid)
        )


# --- EVENT SCHEDULE CONFIG ---

class EventScheduleConfigView(BaseView):
    def __init__(self, cog, reminder_data, guild, editing_rid=None):
        super().__init__(timeout=600)
        self.cog = cog
        self.data = reminder_data
        self.guild = guild
        self.editing_rid = editing_rid

        event_schedule = reminder_data.get('event_schedule') or {}
        self.event_name = event_schedule.get('event_name', '')
        self.selected_frequency = event_schedule.get('frequency', 'weekly')
        self.selected_hour = event_schedule.get('hour')
        self.selected_minute = event_schedule.get('minute')
        self.selected_days = list(event_schedule.get('days_of_week') or [])
        self.selected_dom = event_schedule.get('day_of_month')
        self.build_ui()

    def build_ui(self):
        self.clear_items()

        name_btn = discord.ui.Button(
            label=f"Event Name: {self.event_name or '(not set)'}",
            style=discord.ButtonStyle.secondary, row=0
        )
        name_btn.callback = self.on_name_button
        self.add_item(name_btn)

        next_btn = discord.ui.Button(label="Next", style=discord.ButtonStyle.primary, row=0)
        next_btn.callback = self.on_next
        self.add_item(next_btn)

        freq_select = discord.ui.Select(
            placeholder="Event frequency...",
            options=[
                discord.SelectOption(label="Weekly", value="weekly", default=self.selected_frequency == 'weekly'),
                discord.SelectOption(label="Biweekly", value="biweekly", default=self.selected_frequency == 'biweekly'),
                discord.SelectOption(label="Monthly", value="monthly", default=self.selected_frequency == 'monthly'),
            ],
            row=1
        )
        freq_select.callback = self.on_frequency
        self.add_item(freq_select)

        if self.selected_frequency in ['weekly', 'biweekly']:
            dow_select = discord.ui.Select(
                placeholder="Day(s) of week for event...",
                options=[
                    discord.SelectOption(label="Monday", value="0", default=0 in self.selected_days),
                    discord.SelectOption(label="Tuesday", value="1", default=1 in self.selected_days),
                    discord.SelectOption(label="Wednesday", value="2", default=2 in self.selected_days),
                    discord.SelectOption(label="Thursday", value="3", default=3 in self.selected_days),
                    discord.SelectOption(label="Friday", value="4", default=4 in self.selected_days),
                    discord.SelectOption(label="Saturday", value="5", default=5 in self.selected_days),
                    discord.SelectOption(label="Sunday", value="6", default=6 in self.selected_days),
                ],
                min_values=1, max_values=7, row=2
            )
            dow_select.callback = self.on_days
            self.add_item(dow_select)
        elif self.selected_frequency == 'monthly':
            dom_select = discord.ui.Select(
                placeholder="Day of month for event...",
                options=[discord.SelectOption(label=f"Day {i}", value=str(i), default=self.selected_dom == i) for i in range(1, 26)],
                row=2
            )
            dom_select.callback = self.on_dom
            self.add_item(dom_select)

        hour_select = discord.ui.Select(
            placeholder="Event hour (0-23)...",
            options=[discord.SelectOption(label=f"{i:02d}:00", value=str(i), default=self.selected_hour == i) for i in range(24)],
            row=3
        )
        hour_select.callback = self.on_hour
        self.add_item(hour_select)

        minute_select = discord.ui.Select(
            placeholder="Event minute...",
            options=[discord.SelectOption(label=f":{i:02d}", value=str(i), default=self.selected_minute == i) for i in [0, 15, 30, 45]],
            row=4
        )
        minute_select.callback = self.on_minute
        self.add_item(minute_select)

    async def on_name_button(self, interaction: discord.Interaction):
        await interaction.response.send_modal(EventNameModal(self))

    async def on_frequency(self, interaction: discord.Interaction):
        self.selected_frequency = interaction.data['values'][0]
        self.selected_days = []
        self.selected_dom = None
        self.build_ui()
        await interaction.response.edit_message(view=self)

    async def on_days(self, interaction: discord.Interaction):
        self.selected_days = [int(v) for v in interaction.data['values']]
        await interaction.response.defer()

    async def on_dom(self, interaction: discord.Interaction):
        self.selected_dom = int(interaction.data['values'][0])
        await interaction.response.defer()

    async def on_hour(self, interaction: discord.Interaction):
        self.selected_hour = int(interaction.data['values'][0])
        await interaction.response.defer()

    async def on_minute(self, interaction: discord.Interaction):
        self.selected_minute = int(interaction.data['values'][0])
        await interaction.response.defer()

    async def on_next(self, interaction: discord.Interaction):
        if self.selected_hour is None or self.selected_minute is None:
            return await interaction.response.send_message("Please select both hour and minute for the event.", ephemeral=True)
        if self.selected_frequency in ['weekly', 'biweekly'] and not self.selected_days:
            return await interaction.response.send_message("Please select at least one day of the week.", ephemeral=True)
        if self.selected_frequency == 'monthly' and self.selected_dom is None:
            return await interaction.response.send_message("Please select a day of month.", ephemeral=True)

        guild_tz = self.cog.get_guild_timezone(interaction.guild.id)
        time_local = f"{self.selected_hour:02d}:{self.selected_minute:02d}"
        time_utc = self.cog._convert_time_to_utc(time_local, guild_tz)

        event_schedule = {
            'frequency': self.selected_frequency,
            'time_utc': time_utc,
            'hour': self.selected_hour,
            'minute': self.selected_minute,
            'creation_timezone': str(guild_tz),
            'event_name': self.event_name
        }
        if self.selected_frequency in ['weekly', 'biweekly']:
            event_schedule['days_of_week'] = self.selected_days
        elif self.selected_frequency == 'monthly':
            event_schedule['day_of_month'] = self.selected_dom

        self.data['event_schedule'] = event_schedule
        self.data.pop('event_timestamp_utc', None)
        self.data.pop('event_name', None)

        await interaction.response.edit_message(
            content=None,
            embed=discord.Embed(title="Configuration", description="Select channels, roles, and appearance.", color=0x2B2D31),
            view=ReminderConfigView(self.cog, self.data, self.guild, editing_rid=self.editing_rid)
        )


# --- EVENT NAME MODAL ---

class EventNameModal(discord.ui.Modal):
    def __init__(self, parent_view):
        super().__init__(title="Event Name")
        self.parent_view = parent_view
        self.name_input = discord.ui.TextInput(
            label="Event Name",
            placeholder="e.g. Movie Night, Weekly Scrims",
            default=parent_view.event_name,
            required=False, max_length=100
        )
        self.add_item(self.name_input)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"EventNameModal error: {error}", exc_info=True)
        if not interaction.response.is_done():
            await interaction.response.send_message(f"Error: {error}", ephemeral=True)

    async def on_submit(self, interaction: discord.Interaction):
        self.parent_view.event_name = self.name_input.value.strip()
        self.parent_view.build_ui()
        await interaction.response.edit_message(view=self.parent_view)


# --- REMINDER CONFIG VIEW ---

class ReminderConfigView(BaseView):
    def __init__(self, cog, data, guild, editing_rid=None):
        super().__init__(timeout=600)
        self.cog = cog
        self.data = data
        self.guild = guild
        self.editing_rid = editing_rid

        valid_cids = [c for c in data.get('channel_ids', []) if guild.get_channel(c)]
        self.data['channel_ids'] = valid_cids

        # Row 0: Channel select
        self.channels = discord.ui.ChannelSelect(
            placeholder="Select channels...",
            min_values=1, max_values=25,
            channel_types=[discord.ChannelType.text, discord.ChannelType.news, discord.ChannelType.public_thread],
            default_values=[discord.Object(id=c) for c in valid_cids],
            row=0
        )

        async def on_channel(i):
            self.data['channel_ids'] = [c.id for c in self.channels.values]
            await i.response.defer()

        self.channels.callback = on_channel
        self.add_item(self.channels)

        # Row 1: Ping role
        self.ping = discord.ui.RoleSelect(
            placeholder="Ping Role (optional)...",
            min_values=0, max_values=1,
            default_values=[discord.Object(id=data['ping_role_id'])] if data.get('ping_role_id') else [],
            row=1
        )

        async def on_ping(i):
            self.data['ping_role_id'] = self.ping.values[0].id if self.ping.values else None
            await i.response.defer()

        self.ping.callback = on_ping
        self.add_item(self.ping)

        # Row 2: Color
        self.colors = discord.ui.Select(
            placeholder="Embed Color...",
            options=[
                discord.SelectOption(label=n, value=str(v), default=v == data.get('color'))
                for n, v in EMBED_COLORS.items()
            ],
            row=2
        )

        async def on_color(i):
            self.data['color'] = int(self.colors.values[0])
            await i.response.defer()

        self.colors.callback = on_color
        self.add_item(self.colors)

        # Row 3: Reaction role | Timestamp toggle
        rr_btn = discord.ui.Button(label="Reaction Role Button", style=discord.ButtonStyle.secondary, row=3)
        rr_btn.callback = self.on_reaction_role
        self.add_item(rr_btn)

        ts_enabled = data.get('use_timestamp', False)
        self.timestamp_btn = discord.ui.Button(
            label=f"Dynamic Timestamp [{'ON' if ts_enabled else 'OFF'}]",
            style=discord.ButtonStyle.primary if ts_enabled else discord.ButtonStyle.secondary,
            row=3
        )
        self.timestamp_btn.callback = self.on_timestamp
        self.add_item(self.timestamp_btn)

        # Row 4: Preview | Save | Back
        preview_btn = discord.ui.Button(label="Preview", style=discord.ButtonStyle.secondary, row=4)
        preview_btn.callback = self.on_preview
        self.add_item(preview_btn)

        save_btn = discord.ui.Button(label="Save", style=discord.ButtonStyle.success, row=4)
        save_btn.callback = self.do_save
        self.add_item(save_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=4)
        back_btn.callback = self.on_back
        self.add_item(back_btn)

    async def on_reaction_role(self, interaction: discord.Interaction):
        await interaction.response.send_modal(ReactionRoleConfigModal(self.cog, self.data, self.guild))

    async def on_timestamp(self, interaction: discord.Interaction):
        self.data['use_timestamp'] = not self.data.get('use_timestamp', False)
        ts_on = self.data['use_timestamp']
        self.timestamp_btn.label = f"Dynamic Timestamp [{'ON' if ts_on else 'OFF'}]"
        self.timestamp_btn.style = discord.ButtonStyle.primary if ts_on else discord.ButtonStyle.secondary
        await interaction.response.edit_message(view=self)

    async def on_preview(self, interaction: discord.Interaction):
        if self.channels.values:
            self.data['channel_ids'] = [c.id for c in self.channels.values]
        preview_view = self.cog.build_view(self.data)
        content = self.cog.build_content(self.data) or None
        await interaction.response.send_message(
            content=content, embed=self.cog.build_embed(self.data), view=preview_view, ephemeral=True
        )

    async def on_back(self, interaction: discord.Interaction):
        if self.data.get('type') == 'scheduled':
            await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(title="Add Event Timestamp?", description="Add a recurring event timestamp?", color=0x2B2D31),
                view=EventTimestampChoiceView(self.cog, self.data, self.guild, editing_rid=self.editing_rid)
            )
        else:
            guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
            await interaction.response.edit_message(
                content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
            )

    async def do_save(self, interaction: discord.Interaction):
        if self.channels.values:
            self.data['channel_ids'] = [c.id for c in self.channels.values]
        if not self.data.get('channel_ids'):
            return await interaction.response.send_message("Please select at least one channel.", ephemeral=True)
        await self.cog.save_final(interaction, self.data)


# --- REACTION ROLE VIEWS ---

class ReactionRoleView(BaseView):
    def __init__(self, reaction_role_data):
        super().__init__(timeout=None)
        self.add_item(ReactionRoleButton(reaction_role_data))


class ReactionRoleButton(discord.ui.Button):
    def __init__(self, reaction_role_data):
        style_map = {
            'primary': discord.ButtonStyle.primary,
            'secondary': discord.ButtonStyle.secondary,
            'success': discord.ButtonStyle.success,
            'danger': discord.ButtonStyle.danger
        }
        style = style_map.get(reaction_role_data.get('button_style', 'secondary'), discord.ButtonStyle.secondary)
        emoji = reaction_role_data.get('button_emoji') or None
        super().__init__(
            style=style,
            label=reaction_role_data.get('button_label', 'Get Role'),
            custom_id=f"remind_role:{reaction_role_data['role_id']}",
            emoji=emoji
        )


# --- SKIP VIEW ---

class SkipView(BaseView):
    def __init__(self, cog, rid, guild):
        super().__init__(timeout=300)
        self.cog = cog
        self.rid = rid
        self.guild = guild

        data = cog.reminders.get(rid, {})
        occurrences = get_next_occurrences(data, n=8)

        # Row 0: multi-select of upcoming occurrences
        if occurrences:
            skipped_dates = data.get('skipped_dates', [])
            occ_options = []
            for dt in occurrences:
                date_str = dt.strftime('%Y-%m-%d')
                label = dt.strftime('%a %b %d, %Y')
                occ_options.append(discord.SelectOption(
                    label=label, value=date_str, default=date_str in skipped_dates
                ))
            self.occ_select = discord.ui.Select(
                placeholder="Select dates to skip...",
                options=occ_options,
                min_values=0, max_values=len(occ_options),
                row=0
            )
            self.occ_select.callback = self.on_dates_selected
            self.add_item(self.occ_select)
        else:
            self.occ_select = None

        # Row 1: +N count buttons
        for count in [1, 2, 3, 5, 10]:
            btn = discord.ui.Button(label=f"+{count}", style=discord.ButtonStyle.secondary, row=1)
            btn.callback = self._make_count_callback(count)
            self.add_item(btn)

        # Row 2: clear + back
        clear_count_btn = discord.ui.Button(label="Clear Count", style=discord.ButtonStyle.danger, row=2)
        clear_count_btn.callback = self.on_clear_count
        self.add_item(clear_count_btn)

        clear_dates_btn = discord.ui.Button(label="Clear Date Skips", style=discord.ButtonStyle.danger, row=2)
        clear_dates_btn.callback = self.on_clear_dates
        self.add_item(clear_dates_btn)

        clear_all_btn = discord.ui.Button(label="Clear All", style=discord.ButtonStyle.danger, row=2)
        clear_all_btn.callback = self.on_clear_all
        self.add_item(clear_all_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=2)
        back_btn.callback = self.on_back
        self.add_item(back_btn)

    async def on_dates_selected(self, interaction: discord.Interaction):
        d = self.cog.reminders.get(self.rid)
        if not d:
            return await interaction.response.edit_message(content="Reminder no longer exists.", embed=None, view=None)
        d['skipped_dates'] = list(self.occ_select.values)
        self.cog.save_reminders()
        await interaction.response.edit_message(
            embed=build_skip_embed(d), view=SkipView(self.cog, self.rid, self.guild)
        )

    def _make_count_callback(self, count):
        async def callback(interaction: discord.Interaction):
            d = self.cog.reminders.get(self.rid)
            if not d:
                return await interaction.response.edit_message(content="Reminder not found.", embed=None, view=None)
            current = d.get('skip_next', 0)
            try:
                current = int(current) if isinstance(current, str) else (current or 0)
            except (ValueError, TypeError):
                current = 0
            d['skip_next'] = current + count
            self.cog.save_reminders()
            await interaction.response.edit_message(
                embed=build_skip_embed(d), view=SkipView(self.cog, self.rid, self.guild)
            )
        return callback

    async def on_clear_count(self, interaction: discord.Interaction):
        d = self.cog.reminders.get(self.rid)
        if d:
            d['skip_next'] = 0
            self.cog.save_reminders()
        embed = build_skip_embed(d) if d else discord.Embed(title="Not found")
        await interaction.response.edit_message(embed=embed, view=SkipView(self.cog, self.rid, self.guild))

    async def on_clear_dates(self, interaction: discord.Interaction):
        d = self.cog.reminders.get(self.rid)
        if d:
            d['skipped_dates'] = []
            self.cog.save_reminders()
        embed = build_skip_embed(d) if d else discord.Embed(title="Not found")
        await interaction.response.edit_message(embed=embed, view=SkipView(self.cog, self.rid, self.guild))

    async def on_clear_all(self, interaction: discord.Interaction):
        d = self.cog.reminders.get(self.rid)
        if d:
            d['skip_next'] = 0
            d['skipped_dates'] = []
            self.cog.save_reminders()
        embed = build_skip_embed(d) if d else discord.Embed(title="Not found")
        await interaction.response.edit_message(embed=embed, view=SkipView(self.cog, self.rid, self.guild))

    async def on_back(self, interaction: discord.Interaction):
        d = self.cog.reminders.get(self.rid)
        if not d:
            guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
            return await interaction.response.edit_message(
                content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
            )
        await interaction.response.edit_message(
            content=None, embed=build_detail_embed(self.cog, self.rid, d),
            view=ReminderDetailView(self.cog, self.rid, self.guild)
        )


# --- EDIT CHANNELS VIEW ---

class EditChannelsView(BaseView):
    def __init__(self, cog, rid, data, guild):
        super().__init__(timeout=300)
        self.cog = cog
        self.rid = rid
        self.data = data
        self.guild = guild

        valid_cids = [c for c in data.get('channel_ids', []) if guild.get_channel(c)]
        self.channels = discord.ui.ChannelSelect(
            placeholder="Select channels...",
            min_values=1, max_values=25,
            channel_types=[discord.ChannelType.text, discord.ChannelType.news, discord.ChannelType.public_thread],
            default_values=[discord.Object(id=c) for c in valid_cids],
            row=0
        )

        async def on_channel(i):
            self.data['channel_ids'] = [c.id for c in self.channels.values]
            await i.response.defer()

        self.channels.callback = on_channel
        self.add_item(self.channels)

        save_btn = discord.ui.Button(label="Save", style=discord.ButtonStyle.success, row=1)
        save_btn.callback = self.do_save
        self.add_item(save_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.on_back
        self.add_item(back_btn)

    async def do_save(self, interaction: discord.Interaction):
        new_cids = [c.id for c in self.channels.values] if self.channels.values else self.data.get('channel_ids', [])
        if not new_cids:
            return await interaction.response.send_message("Please select at least one channel.", ephemeral=True)

        d = self.cog.reminders.get(self.rid)
        if not d:
            guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
            return await interaction.response.edit_message(
                content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
            )

        if d.get('type') == 'sticky':
            old_cids = set(d.get('channel_ids', []))
            removed_cids = old_cids - set(new_cids)
            for cid in removed_cids:
                sid = d.get('last_sticky_ids', {}).get(str(cid))
                if sid:
                    ch = interaction.guild.get_channel(cid)
                    if ch:
                        try:
                            old_msg = await ch.fetch_message(sid)
                            await old_msg.delete()
                        except Exception:
                            pass
                d.get('last_sticky_ids', {}).pop(str(cid), None)

        d['channel_ids'] = new_cids
        self.cog.save_reminders()
        await interaction.response.edit_message(
            content=None, embed=build_detail_embed(self.cog, self.rid, d),
            view=ReminderDetailView(self.cog, self.rid, self.guild)
        )

    async def on_back(self, interaction: discord.Interaction):
        d = self.cog.reminders.get(self.rid)
        if not d:
            guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
            return await interaction.response.edit_message(
                content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
            )
        await interaction.response.edit_message(
            content=None, embed=build_detail_embed(self.cog, self.rid, d),
            view=ReminderDetailView(self.cog, self.rid, self.guild)
        )


# --- SETTINGS VIEW ---

class ReminderSettingsView(BaseView):
    def __init__(self, cog, guild):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild = guild

        config = cog.get_guild_config(guild.id)
        current_role_ids = config.get('admin_role_ids', [])

        self.role_select = discord.ui.RoleSelect(
            placeholder="Admin roles (can manage reminders)...",
            min_values=0, max_values=10,
            default_values=[discord.Object(id=rid) for rid in current_role_ids if guild.get_role(rid)],
            row=0
        )
        self.role_select.callback = self.on_roles
        self.add_item(self.role_select)

        tz_btn = discord.ui.Button(
            label=f"Set Timezone (Current: {config.get('timezone', 'UTC')})",
            style=discord.ButtonStyle.primary, row=1
        )
        tz_btn.callback = self.on_timezone
        self.add_item(tz_btn)

        back_btn = discord.ui.Button(label="Back to Panel", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.on_back
        self.add_item(back_btn)

    async def on_roles(self, interaction: discord.Interaction):
        selected_role_ids = [r.id for r in self.role_select.values]
        config = self.cog.get_guild_config(interaction.guild.id)
        config['admin_role_ids'] = selected_role_ids
        self.cog.save_config()
        role_mentions = [f"<@&{rid}>" for rid in selected_role_ids]
        embed = discord.Embed(
            title="Reminder Settings",
            description=(
                f"**Timezone:** {config.get('timezone', 'UTC')}\n"
                f"**Admin Roles:** {', '.join(role_mentions) if role_mentions else 'None'}"
            ),
            color=0x2B2D31
        )
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_timezone(self, interaction: discord.Interaction):
        config = self.cog.get_guild_config(interaction.guild.id)
        await interaction.response.send_modal(TimezoneModal(self.cog, config.get('timezone', 'UTC'), self.guild))

    async def on_back(self, interaction: discord.Interaction):
        guild_reminders = {k: v for k, v in self.cog.reminders.items() if v.get('guild_id') == self.guild.id}
        await interaction.response.edit_message(
            content=None, embed=build_panel_embed(guild_reminders), view=ReminderPanelView(self.cog, self.guild)
        )


# --- TIMEZONE MODAL ---

class TimezoneModal(discord.ui.Modal):
    def __init__(self, cog, current_tz, guild):
        super().__init__(title="Set Timezone")
        self.cog = cog
        self.guild = guild
        self.tz_input = discord.ui.TextInput(
            label="Timezone",
            placeholder="America/New_York, America/Los_Angeles, UTC",
            default=current_tz, required=True, max_length=50
        )
        self.add_item(self.tz_input)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"TimezoneModal error: {error}", exc_info=True)
        if not interaction.response.is_done():
            await interaction.response.send_message(f"Error: {error}", ephemeral=True)

    async def on_submit(self, interaction: discord.Interaction):
        tz_str = self.tz_input.value.strip()
        try:
            ZoneInfo(tz_str)
        except Exception:
            common_tzs = [
                "America/New_York (EST/EDT)", "America/Chicago (CST/CDT)",
                "America/Denver (MST/MDT)", "America/Los_Angeles (PST/PDT)", "UTC"
            ]
            return await interaction.response.send_message(
                "Invalid timezone. Common options:\n" + "\n".join(f"- {tz}" for tz in common_tzs),
                ephemeral=True
            )
        config = self.cog.get_guild_config(interaction.guild.id)
        config['timezone'] = tz_str
        self.cog.save_config()
        embed = discord.Embed(
            title="Reminder Settings",
            description=f"**Timezone:** {tz_str}\n**Admin Roles:** {len(config.get('admin_role_ids', []))} configured",
            color=0x2B2D31
        )
        await interaction.response.edit_message(embed=embed, view=ReminderSettingsView(self.cog, self.guild))


# --- REACTION ROLE CONFIG ---

class ReactionRoleConfigModal(discord.ui.Modal):
    def __init__(self, cog, reminder_data, guild):
        super().__init__(title="Reaction Role Button")
        self.cog = cog
        self.data = reminder_data
        self.guild = guild

        current_rr = reminder_data.get('reaction_role') or {}
        self.role_id_input = discord.ui.TextInput(
            label="Role ID", placeholder="Leave blank to remove button",
            default=str(current_rr.get('role_id', '')) if current_rr else '',
            required=False, max_length=20
        )
        self.button_label = discord.ui.TextInput(
            label="Button Label", placeholder="Get Role",
            default=current_rr.get('button_label', 'Get Role') if current_rr else 'Get Role',
            required=False, max_length=80
        )
        self.button_emoji = discord.ui.TextInput(
            label="Button Emoji (optional)", placeholder="Emoji here",
            default=current_rr.get('button_emoji', '') if current_rr else '',
            required=False, max_length=10
        )
        self.add_item(self.role_id_input)
        self.add_item(self.button_label)
        self.add_item(self.button_emoji)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"ReactionRoleConfigModal error: {error}", exc_info=True)
        if not interaction.response.is_done():
            await interaction.response.send_message(f"Error: {error}", ephemeral=True)

    async def on_submit(self, interaction: discord.Interaction):
        role_id_str = self.role_id_input.value.strip()
        if not role_id_str:
            self.data['reaction_role'] = None
            return await interaction.response.send_message("Reaction role button removed.", ephemeral=True)
        if not role_id_str.isdigit():
            return await interaction.response.send_message("Invalid role ID.", ephemeral=True)
        role_id = int(role_id_str)
        role = interaction.guild.get_role(role_id)
        if not role:
            return await interaction.response.send_message("Role not found in this server.", ephemeral=True)
        await interaction.response.send_message(
            f"Select button style for {role.mention}:",
            view=ReactionRoleStyleView(self.cog, self.data, role_id, self.button_label.value or "Get Role", self.button_emoji.value),
            ephemeral=True
        )


class ReactionRoleStyleView(BaseView):
    def __init__(self, cog, reminder_data, role_id, label, emoji):
        super().__init__(timeout=60)
        self.cog = cog
        self.data = reminder_data
        for style_name, style_value, style_enum in [
            ("Primary (Blue)", "primary", discord.ButtonStyle.primary),
            ("Secondary (Gray)", "secondary", discord.ButtonStyle.secondary),
            ("Success (Green)", "success", discord.ButtonStyle.success),
            ("Danger (Red)", "danger", discord.ButtonStyle.danger),
        ]:
            btn = discord.ui.Button(label=style_name, style=style_enum)
            btn.callback = self._make_callback(role_id, label, emoji, style_value)
            self.add_item(btn)

    def _make_callback(self, role_id, label, emoji, style_value):
        async def cb(interaction: discord.Interaction):
            self.data['reaction_role'] = {
                'role_id': role_id,
                'button_label': label,
                'button_emoji': emoji if emoji else None,
                'button_style': style_value
            }
            await interaction.response.edit_message(content=f"Button configured: {label} ({style_value})", view=None)
        return cb


async def setup(bot):
    await bot.add_cog(Reminders(bot))
