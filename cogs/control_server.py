import asyncio
import hmac
import logging
import os
import platform
import traceback
from collections import deque
from pathlib import Path

import discord
from aiohttp import web
from discord.ext import commands

log = logging.getLogger(__name__)

# The cog discovery rules here mirror main.py's setup_hook exactly: a change to
# what counts as a loadable cog there must change here too, or the dashboard's
# cog list would disagree with what the bot actually loads.
_SKIP_SUFFIXES = ("_shared.py", "_fetcher.py", "_sync.py")

# Where main.py writes its rotating log. The tail endpoint reads this file and
# nothing else — it is not a general file-read endpoint.
_LOG_FILE = Path(__file__).resolve().parent.parent / "logs" / "bot.log"


class ControlServer(commands.Cog):
    """A localhost-only HTTP control channel for the web dashboard.

    The dashboard (same Pi, behind the tunnel) calls this to read the bot's live
    state and to perform owner-only actions — reloading a cog, restarting, tail-
    ing the log. It is deliberately *not* reachable from anywhere but the machine
    itself:

    * The socket binds to 127.0.0.1, so nothing off-box can connect at all.
    * Every request must carry the shared bearer token (VIBEY_CONTROL_TOKEN),
      compared in constant time, so no *other* local process can drive the bot
      just by knowing the port.

    If the token isn't set the server never starts — a control channel with no
    secret is worse than none, and the dashboard degrades to a "not configured"
    state rather than talking to an open door.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.token = os.getenv("VIBEY_CONTROL_TOKEN", "").strip()
        self.host = os.getenv("VIBEY_CONTROL_HOST", "127.0.0.1").strip() or "127.0.0.1"
        try:
            self.port = int(os.getenv("VIBEY_CONTROL_PORT", "8765"))
        except ValueError:
            self.port = 8765
        self.start_time = discord.utils.utcnow()
        self._runner: web.AppRunner | None = None

    # --- lifecycle ---------------------------------------------------------
    async def cog_load(self):
        if not self.token:
            log.warning(
                "Control server disabled: VIBEY_CONTROL_TOKEN is not set. "
                "The dashboard's Bot Control section will show 'not configured'."
            )
            return

        app = web.Application(middlewares=[self._auth_middleware])
        app.add_routes(
            [
                web.get("/health", self._health),
                web.get("/cogs", self._list_cogs),
                web.post("/cogs", self._cog_action),
                web.post("/power", self._power),
                web.get("/logs", self._logs),
            ]
        )
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        log.info("Control server listening on http://%s:%s", self.host, self.port)

    async def cog_unload(self):
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    # --- auth --------------------------------------------------------------
    @web.middleware
    async def _auth_middleware(self, request: web.Request, handler):
        header = request.headers.get("Authorization", "")
        expected = f"Bearer {self.token}"
        # Constant-time compare so a wrong token can't be recovered by timing.
        if not hmac.compare_digest(header, expected):
            return web.json_response({"error": "unauthorized"}, status=401)
        return await handler(request)

    # --- endpoints ---------------------------------------------------------
    async def _health(self, request: web.Request) -> web.Response:
        member_count = 0
        for guild in self.bot.guilds:
            member_count += guild.member_count or 0

        uptime = (discord.utils.utcnow() - self.start_time).total_seconds()
        latency = self.bot.latency  # seconds; nan before the first heartbeat
        return web.json_response(
            {
                "status": "online" if self.bot.is_ready() else "starting",
                "user": str(self.bot.user) if self.bot.user else None,
                "uptimeSeconds": int(uptime),
                "latencyMs": round(latency * 1000) if latency == latency else None,
                "guilds": len(self.bot.guilds),
                "members": member_count,
                "shards": self.bot.shard_count or 1,
                "loadedCogs": len(self.bot.extensions),
                "pid": os.getpid(),
                "discordPy": discord.__version__,
                "python": platform.python_version(),
            }
        )

    def _discover_cogs(self) -> list[dict]:
        """Every loadable cog on disk, each flagged with whether it's loaded.

        A cog that failed to load at startup shows here as loaded=False; the
        dashboard's reload button is then how the reason (its traceback) is
        surfaced, on demand.
        """
        cogs_dir = Path(__file__).resolve().parent
        found: list[dict] = []
        for entry in sorted(cogs_dir.iterdir(), key=lambda p: p.name.lower()):
            name = entry.name
            if name.startswith("__"):
                continue
            if entry.is_file() and name.endswith(".py"):
                if any(name.endswith(suffix) for suffix in _SKIP_SUFFIXES):
                    continue
                base = name[:-3]
                is_package = False
            elif entry.is_dir() and (entry / "__init__.py").exists():
                base = name
                is_package = True
            else:
                continue

            module = f"cogs.{base}"
            found.append(
                {
                    "name": base,
                    "module": module,
                    "loaded": module in self.bot.extensions,
                    "isPackage": is_package,
                }
            )
        return found

    async def _list_cogs(self, request: web.Request) -> web.Response:
        return web.json_response({"cogs": self._discover_cogs()})

    async def _cog_action(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid JSON body"}, status=400)

        name = str(body.get("name", ""))
        action = str(body.get("action", ""))

        # A cog name is a bare module basename — reject anything that could climb
        # out of cogs/ or name an arbitrary import.
        if not name.replace("_", "").isalnum():
            return web.json_response({"ok": False, "error": "invalid cog name"}, status=400)
        if action not in ("load", "unload", "reload"):
            return web.json_response({"ok": False, "error": "invalid action"}, status=400)

        module = f"cogs.{name.lower()}"
        try:
            if action == "reload":
                # reload_extension raises if it was never loaded, so fall back to
                # a plain load — the same "load whether or not it was up" the
                # dashboard's button implies.
                if module in self.bot.extensions:
                    await self.bot.reload_extension(module)
                else:
                    await self.bot.load_extension(module)
            elif action == "load":
                await self.bot.load_extension(module)
            else:  # unload
                await self.bot.unload_extension(module)
        except commands.ExtensionNotFound:
            return web.json_response(
                {"ok": False, "error": f"No cog named '{name}' exists on disk."}, status=404
            )
        except commands.ExtensionAlreadyLoaded:
            return web.json_response({"ok": True, "note": "already loaded"})
        except commands.ExtensionNotLoaded:
            return web.json_response({"ok": True, "note": "already unloaded"})
        except Exception:
            # The full traceback is the whole point — it's what the dashboard
            # shows inline so a failed reload is debuggable from the browser.
            return web.json_response(
                {"ok": False, "error": traceback.format_exc()}, status=500
            )

        log.info("Control: %s cog '%s'", action, module)
        return web.json_response({"ok": True})

    async def _power(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid JSON body"}, status=400)

        action = str(body.get("action", ""))

        if action == "reload_all":
            reloaded: list[str] = []
            failed: dict[str, str] = {}
            for module in list(self.bot.extensions):
                try:
                    await self.bot.reload_extension(module)
                    reloaded.append(module)
                except Exception as exc:
                    failed[module] = str(exc)
            log.info("Control: reloaded %d cogs, %d failed", len(reloaded), len(failed))
            return web.json_response(
                {"ok": not failed, "reloaded": reloaded, "failed": failed}
            )

        if action == "restart":
            # Answer first, then bring the process down: the bot runs under
            # systemd with Restart=always, so a graceful close is a restart.
            log.info("Control: restart requested — closing the bot")
            asyncio.create_task(self._graceful_restart())
            return web.json_response({"ok": True})

        return web.json_response({"ok": False, "error": "invalid action"}, status=400)

    async def _graceful_restart(self):
        # A beat so the HTTP response is flushed to the dashboard before the
        # gateway connection tears down.
        await asyncio.sleep(0.5)
        await self.bot.close()

    async def _logs(self, request: web.Request) -> web.Response:
        try:
            lines = int(request.query.get("lines", "200"))
        except ValueError:
            lines = 200
        lines = max(1, min(lines, 1000))

        if not _LOG_FILE.exists():
            return web.json_response({"lines": [], "file": str(_LOG_FILE), "available": False})

        try:
            # deque(maxlen) keeps only the tail in memory — the log rotates at
            # 5MB, but reading even that lazily is cheaper than slurping it whole.
            with _LOG_FILE.open("r", encoding="utf-8", errors="replace") as handle:
                tail = deque(handle, maxlen=lines)
        except Exception as exc:
            return web.json_response(
                {"lines": [], "file": str(_LOG_FILE), "available": False, "error": str(exc)}
            )

        return web.json_response(
            {
                "lines": [line.rstrip("\n") for line in tail],
                "file": str(_LOG_FILE),
                "available": True,
            }
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(ControlServer(bot))
