"""
Simple JSON-based cache helpers.
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Dict, Any

LOGGER = logging.getLogger(__name__)


class DiscordThreadCache:
    """Persist Discord thread metadata to a JSON file without requiring a database."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._data: Dict[str, Any] = {"channels": {}, "updated_at": None}
        self._load()

    @property
    def path(self) -> Path:
        return self._path

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._data))

    def update(self, timestamp: str, payload: Dict[str, list]) -> None:
        with self._lock:
            channels = self._data.setdefault("channels", {})
            for channel_id, threads in payload.items():
                if not channel_id:
                    continue
                channel_entry = channels.setdefault(
                    channel_id,
                    {"threads": {}, "last_seen": timestamp},
                )
                channel_entry["last_seen"] = timestamp
                thread_map = channel_entry.setdefault("threads", {})

                for thread in threads:
                    thread_id = thread.get("thread_id")
                    if not thread_id:
                        continue
                    existing = thread_map.get(thread_id, {})
                    merged = {**existing, **thread, "last_seen": timestamp}
                    thread_map[thread_id] = merged

            self._data["updated_at"] = timestamp
            self._save_locked()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _load(self) -> None:
        try:
            if not self._path.is_file():
                return
            content = self._path.read_text(encoding="utf-8")
            if not content.strip():
                return
            loaded = json.loads(content)
            if isinstance(loaded, dict):
                self._data.update(loaded)
        except (OSError, json.JSONDecodeError) as exc:
            LOGGER.warning(
                "Failed to load Discord thread cache from %s: %s",
                self._path,
                exc,
            )

    def _save_locked(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._path.with_suffix(self._path.suffix + ".tmp")
            with tmp_path.open("w", encoding="utf-8") as handle:
                json.dump(self._data, handle, ensure_ascii=True, indent=2)
                handle.flush()
            tmp_path.replace(self._path)
        except OSError as exc:
            LOGGER.warning(
                "Failed to persist Discord thread cache to %s: %s",
                self._path,
                exc,
            )
