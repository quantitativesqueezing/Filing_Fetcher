"""
Helpers to retrieve and normalize SEC filings using secbrowser-related tooling.
"""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Tuple

import requests
from datamule import format_accession
from secsgml import parse_sgml_content_into_memory

from .models import FilingDocument

LOGGER = logging.getLogger(__name__)


class FilingFetchError(RuntimeError):
    """Raised when we cannot download or parse a filing."""


def _decode_value(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    if isinstance(value, dict):
        return {str(_decode_value(k)): _decode_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decode_value(item) for item in value]
    return value


@dataclass
class FilingContentFetcher:
    """Download raw SEC filings and expose their constituent documents."""

    user_agent: str
    timeout: int = 30
    max_retries: int = 3
    backoff_seconds: int = 30
    backoff_cap_seconds: int = 300
    backoff_multiplier: float = 2.0

    def __post_init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept-Encoding": "gzip, deflate",
            }
        )

    def close(self) -> None:
        """Release HTTP resources."""
        self._session.close()

    def fetch(self, cik: str, accession: str) -> Tuple[Dict[str, Any], List[FilingDocument]]:
        """
        Download a filing and return (submission_metadata, documents).

        Parameters
        ----------
        cik:
            Numeric string CIK.
        accession:
            Accession number in dash format (##########-##-######).
        """
        accession_str = str(accession)
        try:
            accession_dash = format_accession(accession_str, "dash")
            accession_no_dash = format_accession(accession_str, "no-dash")
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise FilingFetchError(f"Invalid accession number: {accession}") from exc
        try:
            cik_component = str(int(cik))
        except ValueError:
            cik_component = cik.lstrip("0") or cik

        url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik_component}/"
            f"{accession_no_dash}/{accession_dash}.txt"
        )
        LOGGER.debug("Fetching filing from %s", url)

        attempt = 0
        backoff = self.backoff_seconds
        while True:
            response = self._session.get(url, timeout=self.timeout)
            if response.status_code in (429, 403):
                attempt += 1
                wait_seconds = self._calculate_wait_seconds(response, backoff)
                LOGGER.warning(
                    "SEC rate limit encountered for %s (status %s). Backing off for %ss.",
                    accession_dash,
                    response.status_code,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                if attempt >= self.max_retries:
                    raise FilingFetchError(
                        f"Exceeded retry attempts due to SEC rate limits for accession {accession_dash}."
                    )
                backoff = min(
                    backoff * self.backoff_multiplier,
                    self.backoff_cap_seconds,
                )
                continue

            if not response.ok:
                raise FilingFetchError(
                    f"Failed to download filing {accession_dash} for CIK {cik}: "
                    f"{response.status_code} {response.reason}"
                )
            break

        submission_meta_raw, document_blobs = parse_sgml_content_into_memory(
            bytes_content=response.content, keep_filtered_metadata=True
        )
        submission_meta = _decode_value(submission_meta_raw)

        documents: List[FilingDocument] = []
        doc_descriptors = submission_meta.get("documents", [])
        for idx, blob in enumerate(document_blobs):
            descriptor = doc_descriptors[idx] if idx < len(doc_descriptors) else {}
            documents.append(
                FilingDocument(
                    type=str(descriptor.get("type", "") or "").strip(),
                    sequence=str(descriptor.get("sequence", "") or "").strip(),
                    filename=str(descriptor.get("filename", "") or "").strip(),
                    description=str(descriptor.get("description", "") or "").strip() or None,
                    content=blob,
                )
            )

        return submission_meta, documents

    @staticmethod
    def _calculate_wait_seconds(response: requests.Response, fallback: float) -> float:
        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return fallback

        retry_after = retry_after.strip()
        if retry_after.isdigit():
            return max(float(retry_after), fallback)

        try:
            retry_time = parsedate_to_datetime(retry_after)
            if retry_time.tzinfo is None:
                retry_time = retry_time.replace(tzinfo=timezone.utc)
            delta = (retry_time - datetime.now(timezone.utc)).total_seconds()
            if delta > 0:
                return max(delta, fallback)
        except (TypeError, ValueError):
            pass

        return fallback
