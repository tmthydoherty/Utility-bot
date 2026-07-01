#!/usr/bin/env python3
"""Test the image guesser fetcher by DMing one image per API to the bot owner."""
import asyncio
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import discord

load_dotenv(Path(__file__).parent / ".env")

# Set up logging so we can see what the fetcher is doing
logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")

# Import after dotenv so env vars are available
from cogs.image_guesser_fetcher import ImageFetcher

GUILD_ID = 0  # Use 0 as a test guild — these won't go into the real queue


async def main():
    TOKEN = os.getenv("DISCORD_TOKEN")
    if not TOKEN:
        print("Error: DISCORD_TOKEN not found in .env")
        sys.exit(1)

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)
    fetcher = ImageFetcher()

    # Each test: (label, fetcher method name)
    tests = [
        ("TMDB — Movie", "fetch_movies"),
        ("TMDB — TV Show", "fetch_tv_shows"),
        ("TMDB — Person", "fetch_people"),
        ("IGDB — Video Game", "fetch_video_games"),
        ("Jikan — Anime", "fetch_anime"),
        ("Jikan — Character", "fetch_characters"),
        ("Last.fm — Album", "fetch_albums"),
        ("Unsplash — Location", "fetch_locations"),
        ("Unsplash — Country", "fetch_countries"),
        ("Wikimedia — Historical Event", "fetch_historical_events"),
    ]

    @client.event
    async def on_ready():
        try:
            app_info = await client.application_info()
            owner = app_info.owner
            dm = await owner.create_dm()

            await dm.send("**Image Guesser Fetcher Test** — fetching one image per API...")

            passed = 0
            failed = 0

            for label, method_name in tests:
                await dm.send(f"\n🔄 Testing **{label}**...")
                method = getattr(fetcher, method_name)
                try:
                    added = await method(GUILD_ID, count=1)
                except Exception as e:
                    await dm.send(f"❌ **{label}** — Error: `{e}`")
                    failed += 1
                    continue

                if added == 0:
                    await dm.send(f"⚠️ **{label}** — No image returned (API may have no results)")
                    failed += 1
                    continue

                # Find the most recently added image file in assets
                import aiosqlite
                from cogs.image_guesser_fetcher import DB_PATH
                async with aiosqlite.connect(DB_PATH) as db:
                    db.row_factory = aiosqlite.Row
                    async with db.execute(
                        "SELECT * FROM images WHERE guild_id = ? ORDER BY id DESC LIMIT 1",
                        (GUILD_ID,)
                    ) as c:
                        row = await c.fetchone()

                if not row:
                    await dm.send(f"⚠️ **{label}** — Fetched but no DB row found")
                    failed += 1
                    continue

                img = dict(row)
                file_path = img["file_path"]

                if not os.path.exists(file_path):
                    await dm.send(f"⚠️ **{label}** — DB row exists but file missing: `{file_path}`")
                    failed += 1
                    continue

                # Send the image
                file = discord.File(file_path, filename="test.png")
                await dm.send(
                    f"✅ **{label}**\n"
                    f"**Answer:** {img['answer']}\n"
                    f"**Category:** {img['category']}\n"
                    f"**Hint:** {img.get('hint') or 'None'}",
                    file=file
                )
                passed += 1

                await asyncio.sleep(1)

            await dm.send(
                f"\n**Test complete:** {passed} passed, {failed} failed out of {len(tests)}"
            )

            # Clean up test images from DB
            import aiosqlite
            from cogs.image_guesser_fetcher import DB_PATH
            async with aiosqlite.connect(DB_PATH) as db:
                # Get file paths to delete
                async with db.execute("SELECT file_path FROM images WHERE guild_id = ?", (GUILD_ID,)) as c:
                    rows = await c.fetchall()
                for row in rows:
                    if row[0] and os.path.exists(row[0]):
                        os.remove(row[0])
                await db.execute("DELETE FROM images WHERE guild_id = ?", (GUILD_ID,))
                await db.commit()
            await dm.send("🧹 Test images cleaned up from DB and disk.")

        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
        finally:
            await fetcher.close()
            await client.close()

    await client.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
