import asyncio
import aiohttp
from urllib.parse import quote

async def test():
    name = "xPÖKx"
    tag = "5256"
    url = f"https://api.henrikdev.xyz/valorant/v1/account/{quote(name)}/{quote(tag)}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={'Authorization': ''}) as resp:
            print(resp.status)
            print(await resp.text())

asyncio.run(test())
