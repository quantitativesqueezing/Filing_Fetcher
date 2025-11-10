"""
Utilities for polling Discord forum channels for thread metadata.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlsplit

import requests

from .cache import DiscordThreadCache

LOGGER = logging.getLogger(__name__)

DISCORD_API_BASE = "https://discord.com/api/v10"


class DiscordThreadPoller:
    """Background worker that periodically fetches thread IDs from Discord forum channels."""

    def __init__(
        self,
        webhook_mapping: Dict[str, str],
        *,
        poll_interval_seconds: int = 60,
        bot_token: Optional[str] = None,
        stream=None,
        cache_path: Optional[Path | str] = None,
    ) -> None:
        self._webhooks: Dict[str, Optional[str]] = {}
        self._webhook_origins: Dict[str, Optional[str]] = {}
        for url, channel_id in webhook_mapping.items():
            if not url or not url.strip():
                continue
            cleaned_url = url.strip()
            cleaned_channel = channel_id.strip() if channel_id else None
            self._webhooks[cleaned_url] = cleaned_channel
            self._webhook_origins[cleaned_url] = cleaned_channel
        self._poll_interval_seconds = max(poll_interval_seconds, 1)
        self._bot_token = bot_token
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "FilingFetcher-DiscordThreadPoller/1.0"})
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._channel_cache: Dict[str, str] = {}
        self._stream = stream or sys.stdout
        self._initial_poll_done = False
        resolved_cache_path = self._resolve_cache_path(cache_path)
        self._cache = DiscordThreadCache(resolved_cache_path) if resolved_cache_path else None
        if self._cache:
            LOGGER.info("Discord thread cache path: %s", self._cache.path)

    def start(self) -> None:
        if not self._can_poll(log=not self._initial_poll_done):
            return
        if self._thread and self._thread.is_alive():
            return
        self._initial_poll_done = True
        self._thread = threading.Thread(
            target=self._run_loop,
            name="DiscordThreadPoller",
            daemon=True,
        )
        self._thread.start()
        LOGGER.info(
            "Started Discord thread poller for %d webhook(s) (interval=%ss).",
            len(self._webhooks),
            self._poll_interval_seconds,
        )

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread and thread.is_alive():
            self._thread.join(timeout=self._poll_interval_seconds + 5)
        self._session.close()
        if thread:
            LOGGER.info("Stopped Discord thread poller.")

    def poll_once(self) -> None:
        """Perform a single synchronous poll and emit the JSON payload before entering the loop."""
        if not self._can_poll():
            return
        self._initial_poll_done = True
        try:
            payload = self._gather_threads_payload()
            self._emit_payload(payload)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Unexpected error during initial Discord thread poll.")

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _can_poll(self, *, log: bool = True) -> bool:
        if not self._webhooks:
            if log:
                LOGGER.warning("Discord thread poller disabled: no webhook URLs configured.")
            return False
        if not self._bot_token:
            if log:
                LOGGER.warning(
                    "Discord thread poller disabled: configure DISCORD_BOT_TOKEN or DISCORD_API_TOKEN."
                )
            return False
        return True

    def _resolve_cache_path(self, cache_path: Optional[Path | str]) -> Optional[Path]:
        candidate = cache_path
        if candidate is None:
            candidate = os.environ.get("DISCORD_THREAD_CACHE_PATH")
            if candidate is None:
                candidate = Path.home() / ".filingfetcher" / "discord_threads_cache.json"

        if isinstance(candidate, Path):
            path = candidate
        else:
            candidate_str = str(candidate).strip()
            if not candidate_str:
                return None
            path = Path(candidate_str)

        return path.expanduser()

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                payload = self._gather_threads_payload()
                self._emit_payload(payload)
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("Unexpected error while polling Discord threads.")
            if self._stop_event.wait(timeout=self._poll_interval_seconds):
                break

    def _emit_payload(self, payload: Dict) -> None:
        timestamped = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "threads": payload,
        }
        json_output = json.dumps(timestamped, ensure_ascii=True)
        print(json_output, file=self._stream, flush=True)
        thread_count = sum(len(threads) for threads in payload.values())
        LOGGER.info(
            "Discord thread poll returned %d thread(s) across %d channel(s).",
            thread_count,
            len(payload),
        )
        if self._cache:
            try:
                self._cache.update(timestamped["timestamp"], payload)
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("Failed to update Discord thread cache at %s", self._cache.path)

    def _gather_threads_payload(self) -> Dict[str, list]:
        result: Dict[str, list] = {}
        for url, configured_channel in self._webhooks.items():
            channel_id = self._resolve_channel_id(url, configured_channel)
            fallback_ids = self._collect_fallback_thread_ids(url, configured_channel)
            key_id, threads = self._fetch_threads_for_channel(channel_id, fallback_ids)
            if key_id:
                result[key_id] = threads
        return result

    def _resolve_channel_id(self, webhook_url: str, configured_channel: Optional[str]) -> Optional[str]:
        cached = self._channel_cache.get(webhook_url)
        if cached:
            return cached
        redacted_url = _redact_webhook(webhook_url)

        if configured_channel:
            resolved = self._resolve_configured_channel(configured_channel)
            if resolved:
                self._channel_cache[webhook_url] = resolved
                return resolved

        try:
            response = self._session.get(webhook_url, timeout=10)
            if response.status_code >= 400:
                LOGGER.warning(
                    "Failed to resolve Discord webhook %s (status=%s): %s",
                    redacted_url,
                    response.status_code,
                    response.text,
                )
                return None
            data = response.json()
            channel_id = data.get("channel_id")
            if channel_id:
                self._channel_cache[webhook_url] = channel_id
                if webhook_url not in self._webhook_origins or not self._webhook_origins[webhook_url]:
                    self._webhook_origins[webhook_url] = channel_id
            else:
                LOGGER.warning(
                    "Discord webhook response missing channel_id: %s",
                    redacted_url,
                )
            return channel_id
        except requests.RequestException as exc:
            LOGGER.warning("Error resolving Discord webhook %s: %s", redacted_url, exc)
            return None

    def _resolve_configured_channel(self, channel_id: str) -> Optional[str]:
        channel_id = channel_id.strip()
        if not channel_id:
            return None

        headers = self._build_headers()
        if not headers:
            return channel_id

        url = f"{DISCORD_API_BASE}/channels/{channel_id}"
        try:
            response = self._session.get(url, headers=headers, timeout=10)
        except requests.RequestException as exc:
            LOGGER.warning(
                "Error inspecting Discord channel %s: %s",
                channel_id,
                exc,
            )
            return channel_id

        if response.status_code >= 400:
            LOGGER.warning(
                "Failed to inspect Discord channel %s (status=%s): %s",
                channel_id,
                response.status_code,
                response.text,
            )
            return channel_id

        data = response.json()
        channel_type = data.get("type")
        parent_id = data.get("parent_id")
        resolved_id = data.get("id", channel_id)

        # If the configured ID was a thread, use its parent forum channel ID instead.
        if channel_type in {10, 11, 12} and parent_id:
            LOGGER.debug(
                "Configured Discord channel %s is a thread; using parent forum %s.",
                channel_id,
                parent_id,
            )
            return parent_id
        return resolved_id

    def _collect_fallback_thread_ids(self, webhook_url: str, configured_channel: Optional[str]) -> list[str]:
        fallback_ids: list[str] = []
        if configured_channel:
            fallback_ids.append(configured_channel)
        origin = self._webhook_origins.get(webhook_url)
        if origin and origin not in fallback_ids:
            fallback_ids.append(origin)
        return fallback_ids

    def _fetch_threads_for_channel(
        self,
        channel_id: Optional[str],
        fallback_ids: list[str],
    ) -> tuple[Optional[str], list]:
        headers = self._build_headers()
        aggregated: Dict[str, dict] = OrderedDict()

        if channel_id and headers:
            endpoints = [
                (f"{DISCORD_API_BASE}/channels/{channel_id}/threads/active", "active"),
                (f"{DISCORD_API_BASE}/channels/{channel_id}/threads/archived/public", "public_archived"),
            ]

            for endpoint, source in endpoints:
                url = endpoint
                before: Optional[str] = None
                while True:
                    params = {"before": before} if before else None
                    try:
                        response = self._session.get(url, headers=headers, params=params, timeout=15)
                    except requests.RequestException as exc:
                        LOGGER.warning(
                            "Failed to fetch Discord threads for channel %s (%s): %s",
                            channel_id,
                            source,
                            exc,
                        )
                        return self._build_fallback_threads(channel_id, fallback_ids, headers)
                    if response.status_code == 403:
                        LOGGER.warning(
                            "Permission denied fetching threads for channel %s. "
                            "Ensure the bot token has appropriate scopes.",
                            channel_id,
                        )
                        return self._build_fallback_threads(channel_id, fallback_ids, headers)
                    if response.status_code == 404:
                        LOGGER.warning(
                            "Channel %s not found while fetching threads. Falling back to configured thread IDs (if any).",
                            channel_id,
                        )
                        return self._build_fallback_threads(channel_id, fallback_ids, headers)
                    if response.status_code == 429:
                        retry_after = response.json().get("retry_after", 1)
                        LOGGER.warning(
                            "Rate limited while fetching threads for channel %s. Retrying in %ss.",
                            channel_id,
                            retry_after,
                        )
                        time.sleep(float(retry_after))
                        continue
                    if response.status_code >= 400:
                        LOGGER.warning(
                            "Failed to fetch threads for channel %s (status=%s): %s",
                            channel_id,
                            response.status_code,
                            response.text,
                        )
                        return self._build_fallback_threads(channel_id, fallback_ids, headers)

                    data = response.json()
                    threads = data.get("threads", [])
                    for thread in threads:
                        thread_id = thread.get("id")
                        if not thread_id:
                            continue
                        metadata = thread.get("thread_metadata") or {}
                        aggregated[thread_id] = {
                            "thread_id": thread_id,
                            "name": thread.get("name"),
                            "archived": bool(metadata.get("archived")),
                            "locked": bool(metadata.get("locked")),
                            "created_at": metadata.get("create_timestamp"),
                            "channel_id": channel_id,
                            "source": source,
                        }

                    has_more = data.get("has_more")
                    before = threads[-1]["id"] if has_more and threads else None
                    if not has_more:
                        break

            self._ensure_fallback_threads(channel_id, aggregated, fallback_ids, headers)
            return channel_id, list(aggregated.values())

        return self._build_fallback_threads(channel_id, fallback_ids, headers)

    def _build_fallback_threads(
        self,
        channel_id: Optional[str],
        fallback_ids: list[str],
        headers: Dict[str, str],
    ) -> tuple[Optional[str], list]:
        fallback_map: Dict[str, dict] = OrderedDict()
        for thread_id in fallback_ids:
            details = self._fetch_thread_details(thread_id, headers, parent_channel_id=channel_id)
            if details:
                fallback_map[details["thread_id"]] = details
        key_id = channel_id or (fallback_ids[0] if fallback_ids else None)
        return key_id, list(fallback_map.values())

    def _ensure_fallback_threads(
        self,
        channel_id: str,
        aggregated: Dict[str, dict],
        fallback_ids: list[str],
        headers: Dict[str, str],
    ) -> None:
        for thread_id in fallback_ids:
            if not thread_id or thread_id == channel_id:
                continue
            if thread_id in aggregated:
                continue
            details = self._fetch_thread_details(
                thread_id,
                headers,
                source="configured",
                parent_channel_id=channel_id,
            )
            if details:
                aggregated[details["thread_id"]] = details

    def _fetch_thread_details(
        self,
        thread_id: Optional[str],
        headers: Dict[str, str],
        *,
        source: str = "fallback",
        parent_channel_id: Optional[str] = None,
    ) -> Optional[dict]:
        if not thread_id:
            return None
        if not headers:
            return self._build_minimal_thread(
                thread_id,
                source=source,
                parent_channel_id=parent_channel_id,
            )
        url = f"{DISCORD_API_BASE}/channels/{thread_id}"
        try:
            response = self._session.get(url, headers=headers, timeout=10)
        except requests.RequestException as exc:
            LOGGER.warning("Error fetching Discord thread %s: %s", thread_id, exc)
            return self._build_minimal_thread(
                thread_id,
                source=source,
                parent_channel_id=parent_channel_id,
            )

        if response.status_code == 404:
            LOGGER.warning("Thread %s not found (404). Returning minimal metadata.", thread_id)
            return self._build_minimal_thread(
                thread_id,
                source=source,
                parent_channel_id=parent_channel_id,
            )
        if response.status_code == 403:
            LOGGER.warning("Permission denied accessing thread %s. Returning minimal metadata.", thread_id)
            return self._build_minimal_thread(
                thread_id,
                source=source,
                parent_channel_id=parent_channel_id,
            )
        if response.status_code >= 400:
            LOGGER.warning(
                "Failed to fetch thread %s (status=%s): %s",
                thread_id,
                response.status_code,
                response.text,
            )
            return self._build_minimal_thread(
                thread_id,
                source=source,
                parent_channel_id=parent_channel_id,
            )

        data = response.json()
        metadata = data.get("thread_metadata") or {}
        return {
            "thread_id": data.get("id", thread_id),
            "name": data.get("name"),
            "archived": bool(metadata.get("archived")),
            "locked": bool(metadata.get("locked")),
            "created_at": metadata.get("create_timestamp"),
            "channel_id": data.get("parent_id") or parent_channel_id or data.get("id") or thread_id,
            "source": source,
        }

    def _build_minimal_thread(
        self,
        thread_id: str,
        *,
        source: str,
        parent_channel_id: Optional[str] = None,
    ) -> dict:
        return {
            "thread_id": thread_id,
            "name": None,
            "archived": None,
            "locked": None,
            "created_at": None,
            "channel_id": parent_channel_id or thread_id,
            "source": source,
        }

    def _build_headers(self) -> Dict[str, str]:
        if not self._bot_token:
            return {}
        token = _normalize_token(self._bot_token)
        if not token:
            return {}
        return {
            "Authorization": token,
            "Accept": "application/json",
        }


def _redact_webhook(webhook_url: str) -> str:
    if not webhook_url:
        return "<missing webhook>"
    parsed = urlsplit(webhook_url)
    segments = parsed.path.rstrip("/").split("/")
    if len(segments) >= 4:
        segments[-1] = "<redacted>"
    redacted_path = "/".join(segments)
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{parsed.scheme}://{parsed.netloc}{redacted_path}{query}"


def _normalize_token(token: str) -> str:
    token = token.strip()
    if not token:
        return ""
    if token.lower().startswith(("bot ", "bearer ")):
        return token
    return f"Bot {token}"
