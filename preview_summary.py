"""One-shot script: generate a summary card preview and DM it to the bot owner."""
import asyncio
import os
import sys
import tempfile
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path('.') / '.env')

# Add cogs to path so we can import
sys.path.insert(0, str(Path(__file__).parent))

import discord
from io import BytesIO
from playwright.async_api import async_playwright

# Import the template builder directly
from cogs.mapban import (
    _parse_summary_data, _build_summary_html,
    SUMMARY_TEMPLATE_PATH, FONT_DIR
)

# --- Local map and agent image paths ---
ASSETS_DIR = Path(__file__).parent / "assets"
MAPS_DIR = ASSETS_DIR / "maps"
AGENTS_DIR = ASSETS_DIR / "agents"

MAP_URLS = {name: f"file://{MAPS_DIR / f'{name}.png'}" for name in [
    "Abyss", "Pearl", "Haven", "Bind", "Ascent", "Split",
    "Icebox", "Lotus", "Sunset", "Fracture", "Breeze",
]}

AGENT_URLS = {name: f"file://{AGENTS_DIR / f'{name}.png'}" for name in [
    "Jett", "Reyna", "Omen", "Sage", "Sova", "Cypher",
]}

# Mock session data (Bo3 with 11 maps — 8 bans, 2 picks, 1 decider)
MOCK_SESSION_BO3 = {
    "matchup_name": "Red vs Blue",
    "format": "bo3",
    "captain1_id": 111111111111111111,
    "captain2_id": 222222222222222222,
    "captain1_name": "Player 1",
    "captain2_name": "Player 2",
    "actions": [
        {"type": "ban", "map": "Bind", "captain_id": 111111111111111111, "captain_name": "Player 1"},
        {"type": "ban", "map": "Icebox", "captain_id": 222222222222222222, "captain_name": "Player 2"},
        {"type": "ban", "map": "Sunset", "captain_id": 111111111111111111, "captain_name": "Player 1"},
        {"type": "ban", "map": "Lotus", "captain_id": 222222222222222222, "captain_name": "Player 2"},
        {"type": "pick", "map": "Abyss", "captain_id": 111111111111111111, "captain_name": "Player 1"},
        {"type": "pick", "map": "Pearl", "captain_id": 222222222222222222, "captain_name": "Player 2"},
        {"type": "ban", "map": "Fracture", "captain_id": 222222222222222222, "captain_name": "Player 2"},
        {"type": "ban", "map": "Breeze", "captain_id": 111111111111111111, "captain_name": "Player 1"},
        {"type": "ban", "map": "Split", "captain_id": 222222222222222222, "captain_name": "Player 2"},
        {"type": "ban", "map": "Ascent", "captain_id": 111111111111111111, "captain_name": "Player 1"},
    ],
    "map_pool": ["Abyss", "Pearl", "Haven", "Bind", "Ascent", "Split", "Icebox", "Lotus", "Sunset", "Fracture", "Breeze"],
    "side_selections": {
        "Abyss": {"side": "attack", "chosen_by_name": "Player 2"},
        "Pearl": {"side": "defense", "chosen_by_name": "Player 1"},
        "Haven": {"side": "attack", "chosen_by_name": "Player 1"},
    },
    "agent_bans": {
        "Abyss": {"111111111111111111": "Jett", "222222222222222222": "Reyna"},
        "Pearl": {"111111111111111111": "Omen", "222222222222222222": "Sage"},
        "Haven": {"111111111111111111": "Cypher", "222222222222222222": "Sova"},
    },
    "agent_protects": {
        "Abyss": {"111111111111111111": "Sova", "222222222222222222": "Cypher"},
        "Pearl": {"111111111111111111": "Jett", "222222222222222222": "Reyna"},
        "Haven": {"111111111111111111": "Omen", "222222222222222222": "Sage"},
    },
}

# Mock session data (Bo1 with 11 maps — 10 bans, 1 decider)
MOCK_SESSION_BO1 = {
    "matchup_name": "Red vs Blue",
    "format": "bo1",
    "captain1_id": 111111111111111111,
    "captain2_id": 222222222222222222,
    "captain1_name": "Player 1",
    "captain2_name": "Player 2",
    "actions": [
        {"type": "ban", "map": "Bind", "captain_id": 111111111111111111, "captain_name": "Player 1"},
        {"type": "ban", "map": "Icebox", "captain_id": 222222222222222222, "captain_name": "Player 2"},
        {"type": "ban", "map": "Sunset", "captain_id": 111111111111111111, "captain_name": "Player 1"},
        {"type": "ban", "map": "Lotus", "captain_id": 222222222222222222, "captain_name": "Player 2"},
        {"type": "ban", "map": "Fracture", "captain_id": 111111111111111111, "captain_name": "Player 1"},
        {"type": "ban", "map": "Breeze", "captain_id": 222222222222222222, "captain_name": "Player 2"},
        {"type": "ban", "map": "Split", "captain_id": 111111111111111111, "captain_name": "Player 1"},
        {"type": "ban", "map": "Ascent", "captain_id": 222222222222222222, "captain_name": "Player 2"},
        {"type": "ban", "map": "Pearl", "captain_id": 111111111111111111, "captain_name": "Player 1"},
        {"type": "ban", "map": "Abyss", "captain_id": 222222222222222222, "captain_name": "Player 2"},
    ],
    "map_pool": ["Abyss", "Pearl", "Haven", "Bind", "Ascent", "Split", "Icebox", "Lotus", "Sunset", "Fracture", "Breeze"],
    "side_selections": {
        "Haven": {"side": "defense", "chosen_by_name": "Player 1"},
    },
    "agent_bans": {},
    "agent_protects": {},
}


async def main():
    screenshots = {}
    for label, session in [("Bo3", MOCK_SESSION_BO3), ("Bo1", MOCK_SESSION_BO1)]:
        print(f"\n=== Generating {label} card ===")
        data = _parse_summary_data(session)
        template = SUMMARY_TEMPLATE_PATH.read_text()
        html = _build_summary_html(template, data, MAP_URLS, AGENT_URLS)

        # Write HTML to temp file so Chromium can load file:// image URLs
        tmp = tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w", encoding="utf-8")
        tmp.write(html)
        tmp.close()
        tmp_path = tmp.name

        print("Launching Chromium...")
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(executable_path="/usr/bin/chromium")
        page = await browser.new_page(
            viewport={"width": 960, "height": 540},
            device_scale_factor=2,
        )
        await page.goto(f"file://{tmp_path}")
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(500)

        screenshot = await page.screenshot(type="png")
        await page.close()
        await browser.close()
        await pw.stop()
        Path(tmp_path).unlink(missing_ok=True)

        screenshots[label] = screenshot
        out_path = f"/tmp/summary_preview_{label.lower()}.png"
        Path(out_path).write_bytes(screenshot)
        print(f"Saved to {out_path} ({len(screenshot)} bytes)")

    # Send via Discord DM
    TOKEN = os.getenv("DISCORD_TOKEN")
    if not TOKEN:
        print("No DISCORD_TOKEN found, saved to files only")
        return

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        print(f"Logged in as {client.user}")
        app_info = await client.application_info()
        owner = app_info.owner
        print(f"Sending DMs to owner: {owner} (ID: {owner.id})")

        try:
            dm = await owner.create_dm()
            for label, data in screenshots.items():
                buf = BytesIO(data)
                filename = f"summary_preview_{label.lower()}.png"
                file = discord.File(buf, filename=filename)
                embed = discord.Embed(
                    title=f"Summary Card Preview - {label}",
                    description=f"HTML/Playwright summary card ({label} with 11 maps)",
                    color=0x5865F2,
                )
                embed.set_image(url=f"attachment://{filename}")
                await dm.send(embed=embed, file=file)
                print(f"Sent {label} preview!")
        except Exception as e:
            print(f"Failed to DM: {e}")

        await client.close()

    await client.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
