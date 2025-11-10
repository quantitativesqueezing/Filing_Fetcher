"""Output adapters that surface analysis results to the user (console, JSON, Discord)."""

from __future__ import annotations

import json
import logging
import os
import sys
from copy import deepcopy
from dataclasses import dataclass, field
from functools import lru_cache
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Optional, Protocol, TextIO, Any
from urllib.parse import urljoin

import requests

from .models import AnalysisResult, FilingDocument, FilingEvent

LOGGER = logging.getLogger(__name__)
DEFAULT_USER_AGENT = os.environ.get("SEC_USER_AGENT", "FilingFetcher/0.1 (contact@example.com)")
EDGAR_XSL_MAP = {
    "FORM 3": ["xslF345X02", "xslF345X03", "xslF345X05"],
    "FORM 4": ["xslF345X05"],
    "FORM 5": ["xslF345X05"],
    "SCHEDULE 13D": ["xslSCHEDULE_13D_X01"],
    "SCHEDULE 13D/A": ["xslSCHEDULE_13D_X01"],
    "SCHEDULE 13G": ["xslSCHEDULE_13G_X01"],
    "SCHEDULE 13G/A": ["xslSCHEDULE_13G_X01"],
    "FORM 144": ["xsl144X01"],
    "10-K": [None],
    "10-Q": [None],
    "8-K": [None],
    "S-1": [None],
    "S-3": [None],
    "DEF 14A": [None],
    "SD": [None],
}


class Reporter(Protocol):
    """Reporter interface."""

    def publish(self, event: FilingEvent, analysis: AnalysisResult) -> None:
        ...


@dataclass
class ConsoleReporter:
    """Write structured JSON updates to a text stream (default stdout)."""

    stream: TextIO = field(default_factory=lambda: sys.stdout)

    def publish(self, event: FilingEvent, analysis: AnalysisResult) -> None:
        payload = _build_filing_payload(event, analysis)
        json_output = json.dumps(payload, ensure_ascii=True, indent=2, default=str)
        print(json_output, file=self.stream, flush=True)


@dataclass
class JsonReporter:
    """Emit newline-delimited JSON objects to the provided stream."""

    stream: TextIO = field(default_factory=lambda: sys.stdout)

    def publish(self, event: FilingEvent, analysis: AnalysisResult) -> None:
        payload = _build_filing_payload(event, analysis)
        json.dump(payload, self.stream, ensure_ascii=True, default=str)
        self.stream.write("\n")
        self.stream.flush()


def _build_filing_payload(event: FilingEvent, analysis: AnalysisResult) -> Dict[str, Any]:
    company = event.company
    archive_base = event.sec_archive_base_url()
    cik_stripped = event.normalized_cik()
    accession_stripped = event.accession_no_dashes()
    documents = [_document_metadata(doc, archive_base) for doc in event.documents]

    sec_txt_url = event.sec_txt_url()
    primary_document_raw, folder_code, primary_filename = _determine_primary_document_components(
        event.submission_type,
        event.documents,
        event.submission_metadata,
    )
    index_url = _build_index_url(archive_base, folder_code, primary_filename)
    default_document_url = _fetch_default_document_url(index_url)

    payload: Dict[str, Any] = {
        "received_at": event.received_at.isoformat(timespec="seconds"),
        "accession": event.accession,
        "cik": event.cik,
        "submission": {
            "type": event.submission_type,
            "filing_date": event.filing_date,
        },
        "company": {
            "name": company.name if company else None,
            "cik": company.cik if company else event.cik,
            "tickers": company.tickers if company else [],
            "exchanges": company.exchanges if company else [],
            "sic": company.sic if company else None,
            "category": company.category if company else None,
            "description": company.description if company else None,
        },
        "analysis": {
            "sentiment": {
                "label": analysis.sentiment,
                "score": analysis.sentiment_score,
                "rationale": analysis.sentiment_rationale,
            },
            "highlights": analysis.highlights,
            "eli5_summary": analysis.eli5_summary,
        },
        "insider_activity": {
            "is_notable": analysis.insider_notable,
            "summary": analysis.insider_summary,
        },
        "documents": {
            "count": len(documents),
            "items": documents,
        },
        "urls": {
            "cik": cik_stripped,
            "accession": accession_stripped,
            "sec_txt": sec_txt_url,
            "archive_base": archive_base,
            "index": index_url,
            "default_document": default_document_url,
            "xsl_folder_code": folder_code,
        },
        "primary_document": primary_document_raw,
        "metadata": {
            "submission": event.submission_metadata,
            "rss": event.rss_metadata,
        },
    }

    return payload


def _document_metadata(document: FilingDocument, archive_base: Optional[str]) -> Dict[str, Any]:
    size_bytes = len(document.content) if document.content is not None else None
    doc_url = f"{archive_base}{document.filename}" if archive_base and document.filename else None
    return {
        "type": document.type,
        "sequence": document.sequence,
        "filename": document.filename,
        "description": document.description,
        "size_bytes": size_bytes,
        "url": doc_url,
    }


def _select_primary_document(
    documents: list[FilingDocument],
    submission_metadata: Optional[Dict[str, Any]],
) -> Optional[str]:
    """Return the best candidate HTML/HTM filename to represent the filing."""

    if submission_metadata:
        meta_candidate = (
            submission_metadata.get("primary_document")
            or submission_metadata.get("primaryDocument")
            or submission_metadata.get("PRIMARY_DOCUMENT")
        )
        if isinstance(meta_candidate, str) and meta_candidate.strip():
            return meta_candidate.strip()

    def _html_docs():
        for doc in documents:
            if doc.filename and doc.filename.lower().endswith((".htm", ".html")):
                yield doc

    for doc in _html_docs():
        if doc.sequence and doc.sequence.strip().lstrip("0") in ("1", ""):
            return doc.filename

    for doc in _html_docs():
        return doc.filename

    for doc in documents:
        if doc.filename:
            return doc.filename

    return None


def _build_index_url(
    archive_base: str,
    folder_code: Optional[str],
    filename: Optional[str],
) -> Optional[str]:
    if not archive_base or not filename:
        return None
    folder_segment = folder_code or ""
    return f"{archive_base}{folder_segment}{filename}"


def _fetch_default_document_url(index_url: Optional[str]) -> Optional[str]:
    if not index_url:
        return None
    try:
        response = requests.get(
            index_url,
            headers={"User-Agent": DEFAULT_USER_AGENT},
            timeout=15,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.debug("Failed to fetch index HTML %s: %s", index_url, exc)
        return None

    parser = _FirstDocumentLinkParser()
    parser.feed(response.text)
    href = parser.best_link()
    if not href:
        return None
    return urljoin(index_url, href)


class _FirstDocumentLinkParser(HTMLParser):
    """Capture the first document hyperlink from the SEC index page."""

    def __init__(self) -> None:
        super().__init__()
        self._first_link: Optional[str] = None

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = None
        for key, value in attrs:
            if key.lower() == "href":
                href = value
                break
        if not href:
            return
        if self._first_link is None:
            self._first_link = href

    def best_link(self) -> Optional[str]:
        return self._first_link


def _determine_primary_document_components(
    submission_type: str,
    documents: list[FilingDocument],
    submission_metadata: Optional[Dict[str, Any]],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    original = _select_primary_document(documents, submission_metadata)
    if not original:
        return None, None, None

    folder_code = _extract_folder_from_path(original)
    filename = _extract_filename_from_path(original)

    if not folder_code:
        mapped = _pick_folder_from_mapping(submission_type)
        folder_code = mapped

    return original, folder_code, filename


def _extract_folder_from_path(path: str) -> Optional[str]:
    parts = [segment for segment in path.split("/") if segment]
    if len(parts) <= 1:
        return None
    folder = "/".join(parts[:-1])
    return _normalize_folder_code(folder)


def _extract_filename_from_path(path: str) -> Optional[str]:
    parts = [segment for segment in path.split("/") if segment]
    if not parts:
        return None
    return parts[-1]


def _pick_folder_from_mapping(form_type: Optional[str]) -> Optional[str]:
    if not form_type:
        return None
    codes = EDGAR_XSL_MAP.get(form_type.upper())
    if not codes:
        return None
    for code in codes:
        if code:
            return _normalize_folder_code(code)
    return None


def _normalize_folder_code(folder: Optional[str]) -> Optional[str]:
    if not folder:
        return None
    cleaned = folder.strip("/")
    if not cleaned:
        return None
    return cleaned + "/"


def _parse_webhook_mapping(raw: str | None) -> Dict[str, str]:
    """Parse environment-style webhook mappings of the form "url" => "thread_id"."""

    if not raw:
        return {}

    mapping: Dict[str, str] = {}
    for line in raw.splitlines():
        cleaned = line.strip()
        if not cleaned:
            continue
        if cleaned.startswith("#"):
            continue
        cleaned = cleaned.rstrip(",")
        cleaned = cleaned.strip("[] ")
        if "=>" not in cleaned:
            continue

        left, right = cleaned.split("=>", 1)
        url = left.strip().strip('"\'')
        thread_id = right.strip().strip('"\'')
        if url:
            mapping[url] = thread_id
    return mapping


def _load_webhook_env(env_var: str) -> str | None:
    raw = os.environ.get(env_var)
    if raw and not _looks_truncated(raw):
        return raw

    fallback = _read_dotenv_block(env_var)
    if fallback:
        os.environ[env_var] = fallback
        return fallback

    if raw and _looks_truncated(raw):
        LOGGER.warning(
            "Environment variable %s appears truncated. Check .env formatting for multiline values.",
            env_var,
        )
    return raw


def _looks_truncated(raw: str) -> bool:
    stripped = raw.strip()
    return stripped == "[" or stripped == ""


@lru_cache(maxsize=8)
def _read_dotenv_block(env_var: str) -> Optional[str]:
    candidates = []

    dotenv_path = os.environ.get("FILINGFETCHER_DOTENV_PATH")
    if dotenv_path:
        candidates.append(Path(dotenv_path))

    candidates.append(Path.cwd() / ".env")

    for path in candidates:
        if not path or not path.is_file():
            continue
        value = _extract_multiline_assignment(path, env_var)
        if value:
            return value
    return None


def _extract_multiline_assignment(path: Path, env_var: str) -> Optional[str]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except OSError:
        return None

    prefix = f"{env_var}="
    for index, line in enumerate(lines):
        if not line.startswith(prefix):
            continue

        remainder = line[len(prefix) :].rstrip("\n")
        collected = [remainder] if remainder else []

        if remainder.strip().endswith("]"):
            return "\n".join(collected)

        for cursor in range(index + 1, len(lines)):
            segment = lines[cursor].rstrip("\n")
            collected.append(segment)
            if segment.strip().endswith("]"):
                break
            if segment and "=" in segment and segment.split("=", 1)[0].strip() == env_var:
                break

        value = "\n".join(collected).strip("\n")
        return value or None
    return None


def _render_template(data, context):
    if isinstance(data, dict):
        return {key: _render_template(value, context) for key, value in data.items()}
    if isinstance(data, list):
        return [_render_template(item, context) for item in data]
    if isinstance(data, str):
        try:
            return data.format_map(context)
        except KeyError:
            return data
    return data


def _build_discord_context(event: FilingEvent, analysis: AnalysisResult) -> Dict[str, str]:
    company = event.company
    tickers = ", ".join(company.tickers) if company and company.tickers else "—"
    exchanges = ", ".join(company.exchanges) if company and company.exchanges else "—"
    highlights = analysis.highlights if analysis.highlights else []

    return {
        "company_name": company.name if company and company.name else "Unknown issuer",
        "company_cik": event.cik,
        "tickers": tickers,
        "exchanges": exchanges,
        "submission_type": event.submission_type,
        "filing_date": event.filing_date or "",
        "sentiment_label": analysis.sentiment,
        "sentiment_score": f"{analysis.sentiment_score:+.2f}",
        "sentiment_rationale": analysis.sentiment_rationale,
        "sec_txt_url": event.sec_txt_url(),
        "insider_notable": str(analysis.insider_notable)
        if analysis.insider_notable is not None
        else "",
        "insider_summary": analysis.insider_summary or "",
        "eli5_summary": analysis.eli5_summary or "",
        "highlights": "\n".join(highlights),
        "timestamp": event.received_at.isoformat(timespec="seconds"),
    }


@dataclass
class DiscordReporter:
    """Send filing summaries to one or more Discord webhooks."""

    webhook_mapping: Dict[str, str]
    template_path: Optional[Path] = None
    session: requests.Session = field(default_factory=requests.Session)

    def _load_template(self) -> Optional[dict]:
        if not self.template_path:
            return None
        try:
            with self.template_path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            return None
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid Discord template JSON: {self.template_path}") from exc

    def publish(self, event: FilingEvent, analysis: AnalysisResult) -> None:
        if not self.webhook_mapping:
            raise ValueError("No Discord webhook URLs configured.")

        context = _build_discord_context(event, analysis)
        template = self._load_template()
        if template is not None:
            payload = _render_template(deepcopy(template), context)
        else:
            headline = (
                f"{context['company_name']} ({context['tickers']}) filed {context['submission_type']}"
            )
            details = [
                f"Sentiment: {context['sentiment_label']} ({context['sentiment_score']})",
            ]
            if context["sentiment_rationale"]:
                details.append(f"Basis: {context['sentiment_rationale']}")
            if context["eli5_summary"]:
                details.append(f"ELI5: {context['eli5_summary']}")
            if context["insider_summary"]:
                details.append(f"Insider: {context['insider_summary']}")
            if context["highlights"]:
                details.append(f"Highlights:\n{context['highlights']}")
            details.append(f"Source: {context['sec_txt_url']}")
            payload = {"content": headline + "\n" + "\n".join(details)}

        for url, thread_id in self.webhook_mapping.items():
            if not url:
                continue
            params = {"wait": "true"}
            if thread_id:
                params["thread_id"] = thread_id
            response = self.session.post(url, json=payload, params=params, timeout=15)
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Discord webhook call failed ({response.status_code}): {response.text}"
                )


def load_discord_webhooks(mode: str) -> Dict[str, str]:
    env_var = "DISCORD_WEBHOOK_TEST_URL" if mode == "test" else "DISCORD_WEBHOOK_URL"
    raw = _load_webhook_env(env_var)
    return _parse_webhook_mapping(raw)
