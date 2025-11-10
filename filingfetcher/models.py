"""
Core dataclasses used by the FilingFetcher application.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class CompanyProfile:
    """Normalized metadata for a single company identifier."""

    cik: str
    name: Optional[str] = None
    tickers: List[str] = field(default_factory=list)
    exchanges: List[str] = field(default_factory=list)
    category: Optional[str] = None
    sic: Optional[str] = None
    description: Optional[str] = None

    def belongs_to_exchanges(self, target: Iterable[str]) -> bool:
        """Return True when company trades on at least one of the target exchanges."""
        target_upper = {ex.upper() for ex in target}
        return any(exchange.upper() in target_upper for exchange in self.exchanges)


@dataclass
class FilingDocument:
    """Represents a single document contained within an SEC submission."""

    type: str
    sequence: str
    filename: str
    description: Optional[str]
    content: bytes

    def text(self, encoding: str = "utf-8") -> str:
        """Decode the document into text form."""
        return self.content.decode(encoding, errors="ignore")


@dataclass
class FilingEvent:
    """Normalized payload emitted when a new filing is detected."""

    accession: str
    cik: str
    submission_type: str
    filing_date: Optional[str]
    received_at: datetime
    company: Optional[CompanyProfile]
    documents: List[FilingDocument]
    submission_metadata: Dict[str, Any] = field(default_factory=dict)
    rss_metadata: Optional[Dict[str, Any]] = None

    def normalized_cik(self) -> str:
        """Return the CIK with leading zeros removed (or original if all zeros)."""
        stripped = self.cik.lstrip("0")
        return stripped or self.cik

    def accession_no_dashes(self) -> str:
        """Return accession number without dash separators."""
        return self.accession.replace("-", "")

    def sec_txt_url(self) -> str:
        """Return the canonical SEC URL for the raw submission text."""
        acc_no_dash = self.accession_no_dashes()
        cik_component = self.normalized_cik()
        return (
            f"https://www.sec.gov/Archives/edgar/data/{cik_component}/"
            f"{acc_no_dash}/{self.accession}.txt"
        )

    def sec_archive_base_url(self) -> str:
        """Return the base SEC archive URL for documents in this submission."""
        acc_no_dash = self.accession_no_dashes()
        cik_component = self.normalized_cik()
        return f"https://www.sec.gov/Archives/edgar/data/{cik_component}/{acc_no_dash}/"


@dataclass
class AnalysisResult:
    """Structured output produced by the analyzer."""

    sentiment: str
    sentiment_score: float
    sentiment_rationale: str
    highlights: List[str] = field(default_factory=list)
    insider_notable: Optional[bool] = None
    insider_summary: Optional[str] = None
    eli5_summary: Optional[str] = None
