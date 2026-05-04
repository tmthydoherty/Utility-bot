import json
import os
import asyncio
import logging
from typing import Dict, Any, List, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from discord.ext.commands import Bot

logger = logging.getLogger("ticketing_cog")

# Data file paths
DATA_DIR = "data"
TOPICS_FILE = os.path.join(DATA_DIR, "topics.json")
PANELS_FILE = os.path.join(DATA_DIR, "panels.json")
SURVEY_DATA_FILE = os.path.join(DATA_DIR, "survey_data.json")
SURVEY_SESSIONS_FILE = os.path.join(DATA_DIR, "survey_sessions.json")


def _sanitize_for_json(data: Union[Dict, List]) -> Union[Dict, List]:
    if isinstance(data, dict):
        return {key: _sanitize_for_json(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_sanitize_for_json(item) for item in data]
    elif hasattr(data, 'id'):
        return data.id
    else:
        return data


async def _load_json(bot: "Bot", file_path: str, lock: asyncio.Lock) -> Dict[str, Any]:
    def sync_load():
        if not os.path.exists(file_path):
            return {}
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                return json.loads(content) if content else {}
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    async with lock:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, sync_load)


async def _save_json(bot: "Bot", file_path: str, data: Dict[str, Any], lock: asyncio.Lock):
    """Save JSON with atomic write to prevent corruption."""
    def sync_save():
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        sanitized_data = _sanitize_for_json(data)
        temp_path = file_path + ".tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(sanitized_data, f, indent=4)
        os.replace(temp_path, file_path)

    async with lock:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, sync_save)
