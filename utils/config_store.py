"""Cached JSON config storage.

Several cogs kept their settings in a JSON file and re-read it from disk on
every access — including inside `on_message` and `on_member_update`, so a
single chatty hour meant thousands of open/parse cycles for a file that had
not changed. This keeps the parsed document in memory and only touches the
disk when something is actually written.

The cache is a per-path singleton (see `get_store`), so two cogs sharing a
file share one cache and cannot drift apart.

Read semantics match the naive `json.load` they replace: `read()` hands back a
private copy, so a caller is free to mutate it and then decide not to save.
Callers on a hot path that only need to look at a value should use `peek()`,
which skips the copy and must be treated as read-only.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict

log = logging.getLogger(__name__)

_stores: Dict[str, "JsonStore"] = {}
_stores_lock = asyncio.Lock()


class JsonStore:
    """One JSON file, parsed once and written through on save."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._data: Dict[str, Any] | None = None
        self._lock = asyncio.Lock()

    def _load_from_disk(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError) as e:
            # Matches the old helpers: a corrupt file degrades to empty rather
            # than taking the cog down. The write path is atomic, so this
            # should only ever be seen on a file damaged outside the bot.
            log.error(f"Failed to load {self.path}: {e}")
            return {}

    async def _ensure_loaded(self) -> Dict[str, Any]:
        if self._data is None:
            self._data = await asyncio.to_thread(self._load_from_disk)
        return self._data

    async def read(self) -> Dict[str, Any]:
        """A private copy the caller may freely mutate."""
        async with self._lock:
            return copy.deepcopy(await self._ensure_loaded())

    async def peek(self) -> Dict[str, Any]:
        """The live document — cheap, but the caller must not mutate it."""
        async with self._lock:
            return await self._ensure_loaded()

    async def write(self, data: Dict[str, Any]) -> bool:
        """Persist the document and adopt it as the cache. True if it landed.

        Callers that report success to a user must check the result: a cache
        that reported a save the disk never took would look correct until the
        next restart, then silently lose the setting.
        """
        async with self._lock:
            # Serialised under the lock so a concurrent mutation cannot change
            # the document out from under json.dumps.
            try:
                payload = json.dumps(data, indent=4)
            except (TypeError, ValueError) as e:
                log.error(f"Refusing to save {self.path}, not serialisable: {e}")
                return False

        # Written via a temp file so a crash mid-write cannot truncate the
        # config — the old helpers wrote in place and could leave a half file.
        def _write():
            tmp = f"{self.path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
            os.replace(tmp, self.path)

        try:
            await asyncio.to_thread(_write)
        except OSError as e:
            log.error(f"Failed to save {self.path}: {e}")
            # The disk is now the only thing that knows the truth, so drop the
            # cache rather than let it keep serving the version that failed.
            async with self._lock:
                self._data = None
            return False

        async with self._lock:
            # Copied, not aliased: if the caller keeps mutating the dict it
            # handed us, those edits must not appear in the cache as though
            # they had been saved.
            self._data = copy.deepcopy(data)
        return True

    async def invalidate(self) -> None:
        """Drop the cache so the next read comes from disk."""
        async with self._lock:
            self._data = None


async def get_store(path: str | Path) -> JsonStore:
    """Fetch (or create) the single store for `path`."""
    key = str(Path(path).resolve())
    async with _stores_lock:
        store = _stores.get(key)
        if store is None:
            store = JsonStore(path)
            _stores[key] = store
        return store
