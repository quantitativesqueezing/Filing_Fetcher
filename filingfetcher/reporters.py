"""
Output adapters that surface analysis results to the user.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Protocol, TextIO

from .models import AnalysisResult, FilingEvent


class Reporter(Protocol):
    """Reporter interface."""

    def publish(self, event: FilingEvent, analysis: AnalysisResult) -> None:
        ...


@dataclass
class ConsoleReporter:
    """Write human-readable updates to a text stream."""

    stream: TextIO = field(default_factory=lambda: sys.stdout)

    def publish(self, event: FilingEvent, analysis: AnalysisResult) -> None:
        company_name = event.company.name if event.company else "Unknown issuer"
        tickers = ", ".join(event.company.tickers) if event.company else "—"
        exchanges = ", ".join(event.company.exchanges) if event.company else "—"
        timestamp = event.received_at.isoformat(timespec="seconds")
        score_str = f"{analysis.sentiment_score:+.2f}"

        header = (
            f"[{timestamp}] {event.submission_type} filing from {company_name} "
            f"({tickers}) on {exchanges} -> {analysis.sentiment.upper()} ({score_str})"
        )
        print(header, file=self.stream)

        if event.filing_date:
            print(f"  Filing date: {event.filing_date}", file=self.stream)
        print(f"  SEC text: {event.sec_txt_url()}", file=self.stream)

        if analysis.sentiment_rationale:
            print(f"  Sentiment basis: {analysis.sentiment_rationale}", file=self.stream)

        if analysis.insider_notable is not None:
            status = "Notable" if analysis.insider_notable else "Not notable"
            print(f"  Insider activity ({status}): {analysis.insider_summary or 'No details'}", file=self.stream)

        if analysis.eli5_summary:
            print(f"  ELI5 summary: {analysis.eli5_summary}", file=self.stream)

        if analysis.highlights:
            print("  Highlights:", file=self.stream)
            for sentence in analysis.highlights:
                print(f"    - {sentence}", file=self.stream)

        print("", file=self.stream)


@dataclass
class JsonReporter:
    """Emit newline-delimited JSON objects to the provided stream."""

    stream: TextIO = field(default_factory=lambda: sys.stdout)

    def publish(self, event: FilingEvent, analysis: AnalysisResult) -> None:
        payload = {
            "timestamp": event.received_at.isoformat(timespec="seconds"),
            "accession": event.accession,
            "cik": event.cik,
            "submission_type": event.submission_type,
            "filing_date": event.filing_date,
            "company": {
                "name": event.company.name if event.company else None,
                "tickers": event.company.tickers if event.company else [],
                "exchanges": event.company.exchanges if event.company else [],
            },
            "sentiment": {
                "label": analysis.sentiment,
                "score": analysis.sentiment_score,
                "rationale": analysis.sentiment_rationale,
                "highlights": analysis.highlights,
            },
            "insider_activity": {
                "is_notable": analysis.insider_notable,
                "summary": analysis.insider_summary,
            },
            "eli5_summary": analysis.eli5_summary,
            "sec_txt_url": event.sec_txt_url(),
        }
        json.dump(payload, self.stream)
        self.stream.write("\n")
        self.stream.flush()
