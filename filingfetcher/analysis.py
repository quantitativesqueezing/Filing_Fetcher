"""
Rule-based analyzers that transform SEC filing content into actionable insights.
"""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET

from .models import AnalysisResult, FilingDocument, FilingEvent
from .utils import html_to_text, normalize_whitespace, split_sentences

LOGGER = logging.getLogger(__name__)

POSITIVE_KEYWORDS: Sequence[Tuple[str, float]] = (
    ("share repurchase", 3.0),
    ("repurchase program", 2.5),
    ("dividend increase", 2.5),
    ("raised guidance", 2.5),
    ("higher guidance", 2.0),
    ("record revenue", 2.0),
    ("record sales", 2.0),
    ("approval", 1.5),
    ("strategic partnership", 1.5),
    ("acquisition", 1.5),
    ("contract award", 1.5),
    ("expansion", 1.0),
    ("launch", 1.0),
    ("upgraded", 1.0),
)

NEGATIVE_KEYWORDS: Sequence[Tuple[str, float]] = (
    ("bankruptcy", 4.0),
    ("delinquent", 2.5),
    ("default", 2.5),
    ("going concern", 3.0),
    ("layoff", 2.0),
    ("restructuring", 1.5),
    ("impairment", 1.5),
    ("downgrade", 1.5),
    ("terminated", 1.5),
    ("termination", 1.5),
    ("withdraw", 1.5),
    ("restatement", 2.0),
    ("material weakness", 3.0),
    ("investigation", 2.0),
    ("loss", 1.0),
    ("decline", 1.0),
)

FORM4_OPEN_MARKET_CODES = {"P", "S"}
FORM4_EXCLUDED_SEC_TITLES = {"option", "warrant", "unit", "right", "rsu", "restricted"}
FORM4_PLAN_INDICATORS = {
    "10b5-1",
    "10b5-1 plan",
    "rule 10b5-1",
}


def _score_keywords_with_details(text: str, keywords: Sequence[Tuple[str, float]]):
    lowered = text.lower()
    total = 0.0
    matches = []
    for phrase, weight in keywords:
        count = lowered.count(phrase)
        if count:
            contribution = weight * count
            total += contribution
            matches.append((phrase, contribution, count))
    return total, matches


def _safe_iter_text(root: ET.Element) -> str:
    try:
        return " ".join(fragment.strip() for fragment in root.itertext())
    except Exception:  # pragma: no cover - defensive
        return ""


def _is_html(document: FilingDocument) -> bool:
    name = document.filename.lower()
    if name.endswith((".htm", ".html")):
        return True
    trimmed = document.content.lstrip()
    return trimmed.startswith(b"<html") or trimmed.startswith(b"<!doctype html")


def _is_xml(document: FilingDocument) -> bool:
    name = document.filename.lower()
    if name.endswith(".xml"):
        return True
    trimmed = document.content.lstrip()
    return trimmed.startswith(b"<?xml")


def _document_plain_text(document: FilingDocument) -> str:
    raw = document.text()
    if _is_html(document):
        return html_to_text(raw)
    if _is_xml(document):
        try:
            root = ET.fromstring(raw)
            return normalize_whitespace(_safe_iter_text(root))
        except ET.ParseError:
            return normalize_whitespace(re.sub(r"<[^>]+>", " ", raw))
    return normalize_whitespace(raw)


def _extract_item_sections(text: str) -> List[Tuple[str, str]]:
    pattern = re.compile(r"Item\s+\d+(?:\.\d+)?", re.IGNORECASE)
    matches = list(pattern.finditer(text))
    sections: List[Tuple[str, str]] = []

    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)

        header_end = text.find(".", match.end())
        if header_end == -1 or header_end > end:
            header_end = match.end()

        header = normalize_whitespace(text[start : header_end + 1])
        body = normalize_whitespace(text[header_end + 1 : end])

        if header and body:
            sections.append((header, body))

    return sections


def _highlight_sentences(text: str, needles: Iterable[str], limit: int = 3) -> List[str]:
    sentences = split_sentences(text)
    sentences = _filter_informative(sentences)
    highlights = []
    lowered_needles = [needle.lower() for needle in needles]
    for sentence in sentences:
        lower_sentence = sentence.lower()
        if any(needle in lower_sentence for needle in lowered_needles):
            highlights.append(sentence)
        if len(highlights) >= limit:
            break
    return highlights


def _filter_informative(sentences: List[str], min_letters: int = 20) -> List[str]:
    filtered = []
    for sentence in sentences:
        letters = sum(ch.isalpha() for ch in sentence)
        digits = sum(ch.isdigit() for ch in sentence)
        if letters >= min_letters and letters >= digits:
            filtered.append(sentence)
    return filtered


def _format_keyword_matches(matches) -> str:
    parts = []
    for phrase, contribution, count in matches:
        contribution_fmt = f"{contribution:.1f}".rstrip("0").rstrip(".")
        parts.append(f"{phrase}×{count} (+{contribution_fmt})")
    return "; ".join(parts)


def _percentage_change(before: float, after: float) -> Optional[float]:
    try:
        before = float(before)
        after = float(after)
    except (TypeError, ValueError):
        return None
    if before == 0:
        return None
    return (after - before) / before * 100


class FilingAnalyzer:
    """Aggregate heuristics that assess filings for sentiment, novelty and summaries."""

    def analyze(self, event: FilingEvent) -> AnalysisResult:
        primary_doc = self._select_primary_document(event)
        if primary_doc:
            primary_text = _document_plain_text(primary_doc)
        else:
            primary_text = ""

        sentiment_label, sentiment_score, sentiment_details = self._classify_sentiment(
            primary_text, event.submission_type
        )

        highlights = _highlight_sentences(
            primary_text,
            [phrase for phrase, _ in POSITIVE_KEYWORDS + NEGATIVE_KEYWORDS],
        )

        insider_notable = None
        insider_summary = None
        if event.submission_type.strip().upper() == "4" and primary_doc is not None:
            insider_notable, insider_summary = self._analyze_form4(primary_doc)

        eli5_summary = None
        if event.submission_type.strip().upper() == "8-K" and primary_doc is not None:
            eli5_summary = self._summarize_8k(event, primary_doc, primary_text)

        rationale = (
            sentiment_details
            if sentiment_details
            else "No clear bullish/bearish keywords detected; treating as informational."
        )

        return AnalysisResult(
            sentiment=sentiment_label,
            sentiment_score=sentiment_score,
            sentiment_rationale=rationale,
            highlights=highlights,
            insider_notable=insider_notable,
            insider_summary=insider_summary,
            eli5_summary=eli5_summary,
        )

    def _select_primary_document(self, event: FilingEvent) -> Optional[FilingDocument]:
        """Choose the document that best represents the filing."""
        if not event.documents:
            return None

        submission_type = (event.submission_type or "").strip().upper()
        for document in event.documents:
            if document.type.strip().upper() == submission_type:
                return document

        for document in event.documents:
            if document.sequence == "1":
                return document

        return event.documents[0]

    def _classify_sentiment(self, text: str, submission_type: str) -> Tuple[str, float, str]:
        pos_score, pos_matches = _score_keywords_with_details(text, POSITIVE_KEYWORDS)
        neg_score, neg_matches = _score_keywords_with_details(text, NEGATIVE_KEYWORDS)
        score = pos_score - neg_score

        label = "neutral"
        threshold = 2.0
        if submission_type.strip().upper() == "8-K":
            threshold = 1.5  # 8-Ks typically contain concentrated information

        if score >= threshold:
            label = "bullish"
        elif score <= -threshold:
            label = "bearish"

        contributions = []
        if pos_matches:
            contributions.append(f"Positive: {_format_keyword_matches(pos_matches)}")
        if neg_matches:
            contributions.append(f"Negative: {_format_keyword_matches(neg_matches)}")

        return label, score, " | ".join(contributions)

    def _analyze_form4(self, document: FilingDocument) -> Tuple[Optional[bool], Optional[str]]:
        raw = document.text()
        try:
            root = ET.fromstring(raw)
        except ET.ParseError:
            LOGGER.warning("Unable to parse Form 4 XML for %s", document.filename)
            return None, "Could not parse Form 4 XML."

        raw_lower = raw.lower()
        aff_plan_flag = "".join(root.findtext(".//aff10b5One", default="")).strip()
        if aff_plan_flag == "1" or any(ind in raw_lower for ind in FORM4_PLAN_INDICATORS):
            return False, "Trade executed under a Rule 10b5-1 plan."

        notable_transactions = []

        for tx in root.findall(".//nonDerivativeTransaction"):
            summary = self._interpret_form4_transaction(tx)
            if summary is None:
                continue
            notable_transactions.append(summary)

        if not notable_transactions:
            return False, "No open-market common stock transactions detected."

        summary_text = " | ".join(notable_transactions)
        return True, summary_text

    def _interpret_form4_transaction(self, element: ET.Element) -> Optional[str]:
        code = (element.findtext("./transactionCoding/transactionCode") or "").strip().upper()
        if code not in FORM4_OPEN_MARKET_CODES:
            return None

        security_title = (
            element.findtext("./securityTitle/value") or ""
        ).strip().lower()
        if any(token in security_title for token in FORM4_EXCLUDED_SEC_TITLES):
            return None

        shares_text = element.findtext("./transactionAmounts/transactionShares/value") or ""
        try:
            shares = float(shares_text.replace(",", ""))
        except ValueError:
            shares = None
        if not shares or shares <= 0:
            return None

        price_text = element.findtext("./transactionAmounts/transactionPricePerShare/value") or ""
        try:
            price = float(price_text.replace(",", ""))
        except ValueError:
            price = None

        acquired_or_disposed = (
            element.findtext("./transactionAmounts/transactionAcquiredDisposedCode/value")
            or ""
        ).strip().upper()

        action = "purchased" if code == "P" or acquired_or_disposed == "A" else "sold"

        following_text_raw = (
            element.findtext("./postTransactionAmounts/sharesOwnedFollowingTransaction/value")
            or ""
        )
        following_text = following_text_raw.replace(",", "")
        prior_text = None
        if shares is not None and following_text:
            try:
                following_val = float(following_text)
                if action == "purchased":
                    prior_val = following_val - shares
                else:
                    prior_val = following_val + shares
                prior_text = f"{prior_val}"
            except ValueError:
                prior_text = None

        change_pct = None
        if prior_text and following_text:
            change_pct = _percentage_change(prior_text, following_text)

        share_str = f"{shares:,.0f} shares" if shares is not None else "an undisclosed number of shares"
        price_str = f" at ${price:,.2f}" if price is not None else ""

        change_str = ""
        if change_pct is not None:
            change_str = f" (stake change of {change_pct:+.1f}%)"

        return f"Insider {action} {share_str}{price_str}{change_str}"

    def _summarize_8k(
        self,
        event: FilingEvent,
        document: FilingDocument,
        plain_text: str,
    ) -> str:
        item_info = event.submission_metadata.get("item-information", [])
        sections = _extract_item_sections(plain_text)

        summary_parts: List[str] = []
        if item_info:
            joined = ", ".join(item_info[:4])
            summary_parts.append(f"This 8-K covers: {joined}.")

        if sections:
            for header, body in sections[:3]:
                body_sentences = _filter_informative(split_sentences(body, limit=5))
                if body_sentences:
                    summary_parts.append(f"{header}: {' '.join(body_sentences[:2])}")
        else:
            fallback_sentences = _filter_informative(split_sentences(plain_text, limit=6))
            if fallback_sentences:
                summary_parts.append(" ".join(fallback_sentences[:3]))

        if not summary_parts:
            summary_parts.append("Unable to distill the 8-K content into a summary.")

        return " ".join(summary_parts)
