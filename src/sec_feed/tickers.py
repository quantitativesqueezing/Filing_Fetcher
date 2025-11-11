"""Utilities for mapping CIKs to exchange tickers using SEC datasets."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Dict, Optional

import requests

COMPANY_TICKERS_URL = os.environ.get(
    "SEC_COMPANY_TICKERS_URL",
    "https://www.sec.gov/files/company_tickers.json",
)
REFRESH_SECONDS = int(os.environ.get("SEC_TICKER_REFRESH_SECONDS", 12 * 60 * 60))


def _normalize_cik(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return None
    trimmed = digits.lstrip("0")
    return trimmed or digits


class CompanyTickerLookup:
    """Lazy loader/cacher for SEC's company_tickers dataset."""

    def __init__(
        self,
        user_agent: str,
        source_url: str = COMPANY_TICKERS_URL,
        refresh_seconds: int = REFRESH_SECONDS,
    ) -> None:
        if not user_agent:
            raise ValueError("User-Agent is required for SEC lookups")
        self.source_url = source_url
        self.refresh_seconds = max(60, refresh_seconds)
        self.logger = logging.getLogger("sec_feed.tickers")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        self._mapping: Dict[str, str] = {}
        self._last_loaded = 0.0
        self._load_if_stale()

    def lookup(self, cik: Optional[str]) -> Optional[str]:
        norm = _normalize_cik(cik)
        if not norm:
            return None
        self._load_if_stale()
        return self._mapping.get(norm)

    def _load_if_stale(self) -> None:
        now = time.time()
        if (now - self._last_loaded) < self.refresh_seconds and self._mapping:
            return
        try:
            response = self.session.get(self.source_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            if not self._mapping:
                self.logger.warning("Unable to download SEC ticker dataset: %s", exc)
            else:
                self.logger.debug("Skipping ticker dataset refresh (%s)", exc)
            return

        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            self.logger.warning("Invalid ticker dataset response: %s", exc)
            return

        mapping: Dict[str, str] = {}
        for entry in data.values():
            cik_val = entry.get("cik_str") or entry.get("cik")
            ticker = entry.get("ticker")
            norm = _normalize_cik(cik_val)
            if norm and ticker:
                mapping[norm] = ticker.strip().upper()

        if mapping:
            self._mapping = mapping
            self._last_loaded = now
            self.logger.info("Loaded %d SEC ticker mappings", len(mapping))


__all__ = ["CompanyTickerLookup"]
