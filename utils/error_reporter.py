import asyncio
import datetime
import traceback
import logging
from io import BytesIO

import discord

logger = logging.getLogger('bot_main.error_reporter')


class ErrorReporter:
    """Centralized error reporter that queues errors and DMs the bot owner periodically."""

    def __init__(self, bot: discord.Client, *, flush_interval: int = 300):
        self.bot = bot
        self.flush_interval = flush_interval  # seconds between DM flushes
        self._queue: list[str] = []
        self._max_queue = 100
        self._task: asyncio.Task | None = None

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._flush_loop())

    def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()

    async def report(self, source: str, error_msg: str, include_traceback: bool = True):
        """Queue an error for the next DM flush.

        Parameters
        ----------
        source : str
            The cog or module name (e.g. "Tracker", "GamePoll").
        error_msg : str
            A short description of what went wrong.
        include_traceback : bool
            Whether to append the current traceback (default True).
        """
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        parts = [f"[{timestamp}] {source}: {error_msg}"]
        if include_traceback:
            tb = traceback.format_exc()
            if tb and tb.strip() != "NoneType: None":
                parts.append(tb)
        full = "\n".join(parts)
        self._queue.append(full)
        if len(self._queue) > self._max_queue:
            self._queue = self._queue[-self._max_queue:]
        logger.error(f"Queued error from {source}: {error_msg}")

    async def _flush_loop(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await asyncio.sleep(self.flush_interval)
                await self._flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error reporter flush failed: {e}")

    async def _flush(self):
        if not self._queue:
            return

        try:
            if not self.bot.owner_id:
                app_info = await self.bot.application_info()
                self.bot.owner_id = app_info.team.owner_id if app_info.team else app_info.owner.id

            owner = await self.bot.fetch_user(self.bot.owner_id)
            if not owner:
                return

            report_content = "\n\n".join(self._queue)
            self._queue.clear()

            if len(report_content) > 1900:
                file_data = BytesIO(report_content.encode('utf-8'))
                await owner.send(
                    "⚠️ **Error Report**",
                    file=discord.File(file_data, filename="error_report.txt"),
                )
            else:
                await owner.send(f"⚠️ **Error Report**\n```\n{report_content}\n```")
        except Exception as e:
            logger.error(f"Failed to send error report DM: {e}")
