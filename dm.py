#!/usr/bin/env python3
"""Send a Discord DM to the bot owner via the Vibey bot.

Usage:
    python dm.py "Your message here"
    echo "piped content" | python dm.py
    python dm.py -f /path/to/file.txt
    python dm.py  # opens interactive prompt
"""
import asyncio
import argparse
import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import discord

# Silence aiohttp shutdown noise
logging.getLogger("asyncio").setLevel(logging.CRITICAL)

load_dotenv(Path(__file__).parent / ".env")


async def send_dm(message: str):
    TOKEN = os.getenv("DISCORD_TOKEN")
    if not TOKEN:
        print("Error: DISCORD_TOKEN not found in .env")
        sys.exit(1)

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        try:
            app_info = await client.application_info()
            owner = app_info.owner
            dm = await owner.create_dm()

            # Discord message limit is 2000 chars — split if needed
            chunks = [message[i:i+1990] for i in range(0, len(message), 1990)]
            for chunk in chunks:
                await dm.send(chunk)

            print(f"DM sent to {owner} ({len(message)} chars, {len(chunks)} message(s))")
        except Exception as e:
            print(f"Failed to send DM: {e}")
        finally:
            await client.close()

    await client.start(TOKEN)


def main():
    parser = argparse.ArgumentParser(description="DM the bot owner via Vibey")
    parser.add_argument("message", nargs="*", help="Message to send")
    parser.add_argument("-f", "--file", help="Read message from a file")
    args = parser.parse_args()

    if args.file:
        message = Path(args.file).read_text().strip()
    elif args.message:
        message = " ".join(args.message)
    elif not sys.stdin.isatty():
        message = sys.stdin.read().strip()
    else:
        print("Type your message (Ctrl+D to send):")
        message = sys.stdin.read().strip()

    if not message:
        print("Error: empty message")
        sys.exit(1)

    asyncio.run(send_dm(message))


if __name__ == "__main__":
    main()
