"""Playwright card renderer.

Mirrors TrackerCardGenerator in cogs/tracker.py — one long-lived chromium
instance, templates cached at startup, pages bounded by a semaphore. Templates
are rendered with str.format(), so every literal brace in their CSS is doubled.
"""

import asyncio
import contextlib
import io
import logging
import typing
from pathlib import Path

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

logger = logging.getLogger('cogs.economy.cards')

FONT_PATH = "/usr/share/fonts/truetype/noto"
TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates"


class EconomyCardGenerator:
    def __init__(self):
        self.browser = None
        self.playwright = None
        self._page_semaphore = asyncio.Semaphore(3)
        self._templates: dict = {}
        self._restart_lock = asyncio.Lock()

    async def initialize(self) -> bool:
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("Playwright not available — economy cards disabled.")
            return False
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                args=['--font-render-hinting=none', '--disable-lcd-text',
                      '--enable-font-antialiasing']
            )
            for path in TEMPLATE_DIR.glob("eco_*.html"):
                self._templates[path.stem] = path.read_text(encoding='utf-8')
            logger.info(f"Economy card generator initialized ({len(self._templates)} templates).")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize economy card generator: {e}")
            return False

    async def close(self):
        try:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception:
            pass
        self.browser = None
        self.playwright = None

    @property
    def ready(self) -> bool:
        """True only if chromium is actually alive.

        A crashed browser leaves the object in place, so testing for `is not
        None` would report healthy forever and every card would fail until the
        whole bot was restarted.
        """
        if self.browser is None:
            return False
        try:
            return self.browser.is_connected()
        except Exception:
            return False

    async def _ensure_browser(self) -> bool:
        """Relaunch chromium if it has died. One attempt, guarded."""
        if self.ready:
            return True
        async with self._restart_lock:
            if self.ready:
                return True
            logger.warning("Chromium is not connected — reinitialising.")
            try:
                await self.close()
            except Exception:
                pass
            return await self.initialize()

    async def render(self, template_name: str, data: dict,
                     width: int = 920) -> typing.Optional[io.BytesIO]:
        template = self._templates.get(template_name)
        if not template:
            logger.error(f"Unknown template: {template_name}")
            return None
        if not await self._ensure_browser():
            return None
        try:
            html = template.format(**data, font_path=FONT_PATH)
        except KeyError as e:
            logger.error(f"Template placeholder missing in {template_name}: {e}")
            return None

        async with self._page_semaphore:
            page = await self.browser.new_page(
                viewport={'width': width, 'height': 600},
                device_scale_factor=2,
            )
            try:
                await page.set_content(html)
                await page.wait_for_timeout(180)
                body_height = await page.evaluate('document.body.scrollHeight')
                await page.set_viewport_size({'width': width, 'height': body_height})
                screenshot = await page.screenshot(type='png')
            except Exception as e:
                logger.error(f"Render failed for {template_name}: {e}")
                return None
            finally:
                with contextlib.suppress(Exception):
                    await page.close()

        return io.BytesIO(screenshot)
