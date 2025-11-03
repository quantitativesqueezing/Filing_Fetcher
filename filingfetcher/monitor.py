"""
Monitoring loop that relies on secbrowser/datamule to surface live SEC filings.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List, Optional

from datamule import Portfolio, format_accession

from .analysis import FilingAnalyzer
from .fetcher import FilingContentFetcher, FilingFetchError
from .metadata import MetadataRepository
from .models import CompanyProfile, FilingEvent
from .reporters import Reporter

LOGGER = logging.getLogger(__name__)


class FilingMonitor:
    """Continuously monitor EDGAR for new submissions and emit analyses."""

    def __init__(
        self,
        metadata_repository: Optional[MetadataRepository],
        fetcher: FilingContentFetcher,
        analyzer: FilingAnalyzer,
        reporter: Reporter,
        target_exchanges: Iterable[str],
        portfolio_path: str,
        polling_interval_seconds: int,
        validation_interval_seconds: int,
        quiet: bool = True,
    ) -> None:
        self.metadata = metadata_repository or MetadataRepository()
        self.fetcher = fetcher
        self.analyzer = analyzer
        self.reporter = reporter
        self.target_exchanges = list(target_exchanges)
        self.quiet = quiet

        self._portfolio = Portfolio(portfolio_path)
        self._polling_interval_ms = max(polling_interval_seconds, 1) * 1000
        self._validation_interval_ms = max(validation_interval_seconds, 1) * 1000

    def start(self) -> None:
        """Begin monitoring until interrupted."""
        LOGGER.info(
            "Starting monitor with polling=%ss validation=%ss",
            self._polling_interval_ms / 1000,
            self._validation_interval_ms / 1000,
        )
        self._portfolio.monitor_submissions(
            data_callback=self._on_new_submissions,
            polling_interval=self._polling_interval_ms,
            validation_interval=self._validation_interval_ms,
            quiet=self.quiet,
        )

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #
    def _on_new_submissions(self, submissions: List[dict]) -> None:
        for submission in submissions:
            try:
                self._process_submission(submission)
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.exception("Failed to process submission %s: %s", submission, exc)

    def _process_submission(self, submission: dict) -> None:
        accession_dash = format_accession(submission["accession"], "dash")
        cik_list = submission.get("ciks", [])

        company_profiles = [
            self.metadata.get(cik)
            for cik in cik_list
            if self.metadata.get(cik) is not None
        ]

        company_profiles = [
            profile
            for profile in company_profiles
            if profile and profile.belongs_to_exchanges(self.target_exchanges)
        ]

        if not company_profiles:
            LOGGER.debug(
                "Skipping accession %s: no company on target exchanges %s",
                accession_dash,
                self.target_exchanges,
            )
            return

        company_profile = self._merge_profiles(company_profiles)
        primary_cik = cik_list[0] if cik_list else company_profile.cik

        try:
            submission_meta, documents = self.fetcher.fetch(primary_cik, accession_dash)
        except FilingFetchError as exc:
            LOGGER.warning(str(exc))
            return

        event = FilingEvent(
            accession=accession_dash,
            cik=primary_cik,
            submission_type=submission.get("submission_type", ""),
            filing_date=submission.get("filing_date"),
            received_at=datetime.now(timezone.utc),
            company=company_profile,
            documents=documents,
            submission_metadata=submission_meta,
            rss_metadata=submission,
        )

        analysis = self.analyzer.analyze(event)
        self.reporter.publish(event, analysis)

    @staticmethod
    def _merge_profiles(profiles: List[CompanyProfile]) -> CompanyProfile:
        primary = profiles[0]
        if len(profiles) == 1:
            return primary

        tickers = sorted({ticker for profile in profiles for ticker in profile.tickers})
        exchanges = sorted({exchange for profile in profiles for exchange in profile.exchanges})

        merged = CompanyProfile(
            cik=primary.cik,
            name=primary.name,
            tickers=tickers,
            exchanges=exchanges,
            category=primary.category,
            sic=primary.sic,
            description=primary.description,
        )
        return merged

