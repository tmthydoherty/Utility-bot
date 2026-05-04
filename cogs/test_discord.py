import discord
import asyncio

class MockConnection:
    pass

async def test():
    emoji = discord.PartialEmoji.from_str("<a:test:12345678901234>")
    emoji._state = MockConnection()
    try:
        await emoji.read()
    except Exception as e:
        print(f"Exception: {type(e).__name__}: {e}")

asyncio.run(test())
