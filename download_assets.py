"""Download all Valorant map and agent images locally."""
import asyncio
import aiohttp
from pathlib import Path


def safe_filename(name: str) -> str:
    """Replace filesystem-unsafe characters."""
    return name.replace("/", "_").replace("\\", "_")

ASSETS_DIR = Path(__file__).parent / "assets"
MAPS_DIR = ASSETS_DIR / "maps"
AGENTS_DIR = ASSETS_DIR / "agents"


async def main():
    MAPS_DIR.mkdir(parents=True, exist_ok=True)
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        # Download maps
        print("Fetching map list...")
        async with session.get("https://valorant-api.com/v1/maps") as r:
            data = await r.json()
            maps = data.get("data", [])

        # Filter to competitive maps only (skip The Range, Skirmish, etc.)
        skip = {"The Range", "Basic Training", "Skirmish A", "Skirmish B",
                "Skirmish C", "District", "Kasbah", "Drift", "Piazza",
                "Glitch"}
        maps = [m for m in maps if m["displayName"] not in skip]

        for m in maps:
            name = m["displayName"]
            url = m.get("splash")
            if not url:
                print(f"  SKIP {name} (no splash)")
                continue
            dest = MAPS_DIR / f"{safe_filename(name)}.png"
            if dest.exists():
                print(f"  EXISTS {name}")
                continue
            print(f"  Downloading {name}...")
            async with session.get(url) as img_r:
                if img_r.status == 200:
                    dest.write_bytes(await img_r.read())
                    print(f"    OK ({dest.stat().st_size} bytes)")
                else:
                    print(f"    FAILED status={img_r.status}")

        # Download agents
        print("\nFetching agent list...")
        async with session.get(
            "https://valorant-api.com/v1/agents?isPlayableCharacter=true"
        ) as r:
            data = await r.json()
            agents = data.get("data", [])

        for a in agents:
            name = a["displayName"]
            url = a.get("displayIcon")
            if not url:
                print(f"  SKIP {name} (no icon)")
                continue
            dest = AGENTS_DIR / f"{safe_filename(name)}.png"
            if dest.exists():
                print(f"  EXISTS {name}")
                continue
            print(f"  Downloading {name}...")
            async with session.get(url) as img_r:
                if img_r.status == 200:
                    dest.write_bytes(await img_r.read())
                    print(f"    OK ({dest.stat().st_size} bytes)")
                else:
                    print(f"    FAILED status={img_r.status}")

    print("\nDone!")
    print(f"Maps:   {len(list(MAPS_DIR.glob('*.png')))} files")
    print(f"Agents: {len(list(AGENTS_DIR.glob('*.png')))} files")


if __name__ == "__main__":
    asyncio.run(main())
