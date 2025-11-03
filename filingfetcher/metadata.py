"""
Utilities for loading and querying company metadata supplied by secbrowser/datamule.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from datamule import load_package_dataset

from .models import CompanyProfile


def _safe_eval_list(value: Optional[str]) -> list[str]:
    """Parse a string that stores a Python-style list, returning [] when parsing fails."""
    if not value:
        return []
    try:
        parsed = ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return []
    if isinstance(parsed, (list, tuple, set)):
        return [str(item).strip() for item in parsed if str(item).strip()]
    if isinstance(parsed, str) and parsed:
        return [parsed.strip()]
    return []


def _normalize_cik(value: str) -> Optional[str]:
    """Normalize the provided CIK string into a canonical numeric string."""
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.lstrip("0") or digits or None


@dataclass
class MetadataRepository:
    """In-memory cache of metadata keyed by CIK."""

    dataset_name: str = "listed_filer_metadata"

    def __post_init__(self) -> None:
        self._by_cik: Dict[str, CompanyProfile] = {}
        self._load()

    def _load(self) -> None:
        """Populate the metadata cache."""
        rows = load_package_dataset(self.dataset_name)
        for row in rows:
            cik = _normalize_cik(row.get("cik"))
            if not cik:
                continue

            exchanges = _safe_eval_list(row.get("exchanges"))
            tickers = _safe_eval_list(row.get("tickers") or row.get("ticker"))

            profile = CompanyProfile(
                cik=cik,
                name=(row.get("name") or row.get("companyName") or "").strip() or None,
                tickers=tickers,
                exchanges=exchanges,
                category=(row.get("category") or "").strip() or None,
                sic=(row.get("sic") or "").strip() or None,
                description=(row.get("description") or "").strip() or None,
            )
            self._by_cik[cik] = profile

    def get(self, cik: str) -> Optional[CompanyProfile]:
        """Return company info for a given CIK if available."""
        key = _normalize_cik(cik)
        if not key:
            return None
        return self._by_cik.get(key)

    def exchanges_for(self, cik: str) -> list[str]:
        """Return the exchanges for the provided CIK."""
        profile = self.get(cik)
        return profile.exchanges if profile else []

    def tickers_for(self, cik: str) -> list[str]:
        """Return the tickers for the provided CIK."""
        profile = self.get(cik)
        return profile.tickers if profile else []

    def filter_by_exchanges(self, ciks: Iterable[str], exchanges: Iterable[str]) -> list[CompanyProfile]:
        """Return company profiles whose listings intersect with target exchanges."""
        target = {exchange.upper() for exchange in exchanges}
        matches = []
        for cik in ciks:
            profile = self.get(cik)
            if not profile:
                continue
            if profile.belongs_to_exchanges(target):
                matches.append(profile)
        return matches
