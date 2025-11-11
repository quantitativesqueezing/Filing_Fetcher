#!/usr/bin/env python3
"""Poll the SEC latest filings feed and parse each new filing."""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import sys
import time
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from datetime import datetime
from urllib.parse import quote_plus
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

# Ensure the local src/ folder is importable without adjusting PYTHONPATH manually.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from sec_feed.parser import fetch_index_documents, to_ndjson, set_user_agent  # noqa: E402
from sec_feed.tickers import CompanyTickerLookup  # noqa: E402


ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
ACCESSION_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")
SUMMARY_RE = re.compile(
    r"Filed:\s*(?P<filed>\d{4}-\d{2}-\d{2}).*?AccNo:\s*(?P<acc>\d{10}-\d{2}-\d{6}).*?Size:\s*(?P<size>[\w\s]+)",
    re.IGNORECASE,
)
DEFAULT_DOC_DELAY = 0.6
DEFAULT_POLL_INTERVAL = 7.0
DEFAULT_FEED_BASE = "https://www.sec.gov/cgi-bin/browse-edgar"
ENV_FILE = REPO_ROOT / ".env"
WEBHOOK_PAIR_RE = re.compile(r"\"([^\"]+)\"\s*=>\s*\"([^\"]*)\"")
PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")
DOLLAR_PLACEHOLDER_RE = re.compile(r"\$\{([^{}]+)\}")
DiscordTarget = Tuple[str, Optional[str]]


def build_feed_url(count: int, owner: str = "exclude", base: str = DEFAULT_FEED_BASE) -> str:
    params = {
        "action": "getcurrent",
        "owner": owner,
        "count": str(max(1, count)),
        "output": "atom",
    }
    return f"{base}?{urlencode(params)}"


def _clean_summary(summary_html: str) -> Dict[str, Optional[str]]:
    if not summary_html:
        return {"raw": None, "text": None, "filed": None, "accession": None, "size": None}
    text = BeautifulSoup(summary_html, "html.parser").get_text(" ", strip=True)
    info: Dict[str, Optional[str]] = {
        "raw": summary_html,
        "text": text,
        "filed": None,
        "accession": None,
        "size": None,
    }
    match = SUMMARY_RE.search(text.replace("\xa0", " "))
    if match:
        info.update(
            {
                "filed": match.group("filed"),
                "accession": match.group("acc"),
                "size": match.group("size").strip(),
            }
        )
    return info


def format_release_timestamp(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    candidate = value.strip()
    iso_candidate = candidate.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_candidate)
    except ValueError:
        return candidate
    date_part = dt.strftime("%a %m/%d/%Y")
    time_part = dt.strftime("%I:%M%p").lstrip("0")
    if not time_part:
        time_part = dt.strftime("%I:%M%p")
    return f"{date_part} - {time_part.lower()}"


def _entry_to_json(entry: ET.Element) -> Dict:
    title = entry.findtext("atom:title", default="", namespaces=ATOM_NS).strip()
    summary_html = entry.findtext("atom:summary", default="", namespaces=ATOM_NS)
    summary = _clean_summary(summary_html)
    link_elem = None
    for candidate in entry.findall("atom:link", ATOM_NS):
        rel = candidate.attrib.get("rel", "alternate")
        if rel == "alternate":
            link_elem = candidate
            break
        if link_elem is None:
            link_elem = candidate
    href = link_elem.attrib.get("href") if link_elem is not None else None
    href_abs = urljoin("https://www.sec.gov", href) if href else None
    entry_id = entry.findtext("atom:id", default="", namespaces=ATOM_NS)
    updated = entry.findtext("atom:updated", default="", namespaces=ATOM_NS)
    release_display = format_release_timestamp(updated)
    form_type = entry.find("atom:category", ATOM_NS)
    form_term = form_type.attrib.get("term") if form_type is not None else None
    title_parts = title.split(" - ", 1)
    form_from_title = title_parts[0].strip() if len(title_parts) == 2 else None
    subject_part = title_parts[1].strip() if len(title_parts) == 2 else title
    company = subject_part.split("(")[0].strip()
    paren_values = re.findall(r"\(([^)]+)\)", subject_part)
    cik = paren_values[0] if paren_values else None
    role = paren_values[1] if len(paren_values) >= 2 else None
    accession = summary.get("accession")
    if not accession:
        accession = _extract_accession([href_abs, entry_id, title])
    return {
        "entry_id": entry_id or None,
        "updated": updated or None,
        "title": title,
        "form_type": form_term or form_from_title,
        "company": company or None,
        "cik": cik,
        "role": role,
        "summary": summary,
        "filing_url": href_abs,
        "accession_number": accession,
        "release_display": release_display,
    }


def _extract_accession(candidates: Iterable[Optional[str]]) -> Optional[str]:
    for candidate in candidates:
        if not candidate:
            continue
        match = ACCESSION_RE.search(candidate)
        if match:
            return match.group(1)
    return None


def _read_env_block(var_name: str) -> Optional[str]:
    direct = os.environ.get(var_name)
    if direct:
        return direct
    if not ENV_FILE.exists():
        return None
    text = ENV_FILE.read_text()
    pattern = re.compile(rf"{var_name}\s*=\s*(\[[^\]]*\])", re.MULTILINE | re.DOTALL)
    match = pattern.search(text)
    if match:
        return match.group(1)
    return None


def _parse_webhook_pairs(raw: Optional[str]) -> List[DiscordTarget]:
    if not raw:
        return []
    cleaned = re.sub(r"#.*", "", raw)
    pairs: List[DiscordTarget] = []
    for url, thread in WEBHOOK_PAIR_RE.findall(cleaned):
        url_clean = url.strip()
        if not url_clean:
            continue
        thread_id = thread.strip() or None
        pairs.append((url_clean, thread_id))
    return pairs


def load_discord_targets(use_test: bool) -> List[DiscordTarget]:
    var_name = "DISCORD_WEBHOOK_TEST_URL" if use_test else "DISCORD_WEBHOOK_URL"
    block = _read_env_block(var_name)
    return _parse_webhook_pairs(block)


class DiscordDispatcher:
    def __init__(self, template_path: Path, targets: List[DiscordTarget], user_agent: str) -> None:
        if not targets:
            raise ValueError("At least one Discord webhook must be configured")
        self.template_path = template_path
        with template_path.open("r", encoding="utf-8") as handle:
            self.template = json.load(handle)
        self.targets = targets
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        self.logger = logging.getLogger("sec_feed.discord")

    def dispatch(self, context: Dict) -> None:
        payload = self._render(context)
        for url, thread in self.targets:
            params: Dict[str, str] = {"wait": "true"}
            if thread:
                params["thread_id"] = thread
            try:
                resp = self.session.post(url, params=params, json=payload, timeout=20)
                resp.raise_for_status()
            except requests.RequestException as exc:
                self.logger.warning("Discord webhook failed (%s): %s", url, exc)

    def _render(self, context: Dict) -> Dict:
        return self._transform(copy.deepcopy(self.template), context)

    def _transform(self, value, context: Dict):
        if isinstance(value, dict):
            return {k: self._transform(v, context) for k, v in value.items()}
        if isinstance(value, list):
            return [self._transform(item, context) for item in value]
        if isinstance(value, str):
            return self._replace_tokens(value, context)
        return value

    def _replace_tokens(self, text: str, context: Dict) -> str:
        text = DOLLAR_PLACEHOLDER_RE.sub(lambda m: self._resolve_path(context, m.group(1)), text)
        text = PLACEHOLDER_RE.sub(lambda m: self._resolve_path(context, m.group(1)), text)
        return text

    def _resolve_path(self, context: Dict, path: str) -> str:
        current = context
        for part in path.split('.'):
            token = part.strip()
            if token == "":
                continue
            if isinstance(current, list):
                try:
                    idx = int(token)
                except ValueError:
                    return ""
                if idx < 0 or idx >= len(current):
                    return ""
                current = current[idx]
            elif isinstance(current, dict):
                current = current.get(token)
            else:
                return ""
            if current is None:
                break
        if current is None:
            return ""
        return str(current)

def _entry_priority(entry: Dict) -> int:
    role = (entry.get("role") or "").lower()
    if "subject" in role:
        return 0
    if "filed by" in role or "filed" in role:
        return 1
    return 2


class LatestFilingsPoller:
    def __init__(
        self,
        feed_url: str,
        user_agent: str,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        document_delay: float = DEFAULT_DOC_DELAY,
        compact: bool = False,
        ticker_lookup: Optional[CompanyTickerLookup] = None,
        dispatcher: Optional[DiscordDispatcher] = None,
        max_results: Optional[int] = None,
    ) -> None:
        if not user_agent:
            raise ValueError("A SEC-compliant user agent string is required.")
        self.feed_url = feed_url
        self.poll_interval = max(1.0, poll_interval)
        self.document_delay = max(0.2, document_delay)
        self.compact = compact
        self.ticker_lookup = ticker_lookup
        self.dispatcher = dispatcher
        self.max_results = max_results if (max_results is None or max_results > 0) else None
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/atom+xml,application/xml",
            }
        )
        self.logger = logging.getLogger("sec_feed.poller")
        self._etag: Optional[str] = None
        self._last_modified: Optional[str] = None
        self._seen_accessions: Set[str] = set()
        self._processed_count = 0

    def run_forever(self) -> None:
        self.logger.info("Polling %s every %.1f seconds", self.feed_url, self.poll_interval)
        while True:
            loop_started = time.monotonic()
            try:
                payload = self._fetch_feed()
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 403:
                    cooldown = max(60.0, self.poll_interval * 20)
                    self.logger.error(
                        "SEC returned 403 Forbidden. Verify your User-Agent (%s) and pausing for %.0f seconds",
                        self.session.headers.get("User-Agent"),
                        cooldown,
                    )
                    time.sleep(cooldown)
                else:
                    self.logger.warning("Feed request failed (%s). Backing off.", exc)
                    time.sleep(self.poll_interval)
                continue
            except requests.RequestException as exc:
                self.logger.warning("Feed request error (%s). Backing off.", exc)
                time.sleep(self.poll_interval)
                continue

            if payload is not None:
                try:
                    feed_json = self._parse_feed(payload)
                except ET.ParseError as exc:
                    self.logger.error("Could not parse feed XML: %s", exc)
                else:
                    new_entries = self._gather_new_entries(feed_json.get("entries", []))
                    if new_entries:
                        self.logger.info("Processing %d new filings", len(new_entries))
                        for entry in new_entries:
                            self._process_entry(entry)
                            if self.max_results and self._processed_count >= self.max_results:
                                self.logger.info("Reached max results (%d); exiting.", self.max_results)
                                return
                            time.sleep(self.document_delay)
                    else:
                        self.logger.debug("No unseen filings in this interval")

            elapsed = time.monotonic() - loop_started
            sleep_for = max(0.0, self.poll_interval - elapsed)
            time.sleep(sleep_for)

    def _fetch_feed(self) -> Optional[str]:
        headers: Dict[str, str] = {}
        if self._etag:
            headers["If-None-Match"] = self._etag
        if self._last_modified:
            headers["If-Modified-Since"] = self._last_modified
        response = self.session.get(self.feed_url, headers=headers, timeout=30)
        if response.status_code == 304:
            self.logger.debug("Feed not modified")
            return None
        response.raise_for_status()
        self._etag = response.headers.get("ETag")
        self._last_modified = response.headers.get("Last-Modified")
        return response.text

    def _parse_feed(self, xml_text: str) -> Dict:
        root = ET.fromstring(xml_text)
        entries = [_entry_to_json(entry) for entry in root.findall("atom:entry", ATOM_NS)]
        feed_meta = {
            "title": root.findtext("atom:title", default="", namespaces=ATOM_NS),
            "id": root.findtext("atom:id", default="", namespaces=ATOM_NS),
            "updated": root.findtext("atom:updated", default="", namespaces=ATOM_NS),
            "entries": entries,
        }
        self.logger.debug("Fetched %d feed entries", len(entries))
        return feed_meta

    def _gather_new_entries(self, entries: List[Dict]) -> List[Dict]:
        grouped: Dict[str, Dict] = {}
        order: List[str] = []
        for entry in entries:
            accession = entry.get("accession_number") or entry.get("entry_id")
            if not accession or accession in self._seen_accessions:
                continue
            priority = _entry_priority(entry)
            existing = grouped.get(accession)
            if existing is None:
                grouped[accession] = {"entry": entry, "priority": priority}
                order.append(accession)
            elif priority < existing["priority"]:
                grouped[accession] = {"entry": entry, "priority": priority}

        fresh: List[Dict] = []
        for accession in order:
            selected = grouped.get(accession)
            if not selected:
                continue
            self._seen_accessions.add(accession)
            fresh.append(selected["entry"])
        fresh.reverse()
        return fresh

    def _process_entry(self, entry: Dict) -> None:
        filing_url = entry.get("filing_url")
        if not filing_url:
            self.logger.debug("Skipping entry without filing URL: %s", entry.get("title"))
            return
        try:
            documents_payload = fetch_index_documents(filing_url)
        except requests.HTTPError as exc:
            self.logger.warning("Failed to fetch filing %s (%s)", filing_url, exc)
            return
        except requests.RequestException as exc:
            self.logger.warning("Network error fetching filing %s (%s)", filing_url, exc)
            return

        ticker = self._resolve_ticker(entry)
        entry["ticker"] = ticker
        documents_payload["ticker"] = ticker
        primary_doc_url = self._primary_document_url(documents_payload) or filing_url
        entry["links"] = self._build_links(primary_doc_url, ticker)
        ndjson_lines = list(to_ndjson(documents_payload))
        output = {
            "feed_entry": entry,
            "documents": documents_payload,
            "ndjson": ndjson_lines,
            "ticker": ticker,
        }
        json_str = json.dumps(output, indent=None if self.compact else 2)
        print(json_str, flush=True)
        if self.dispatcher:
            self.dispatcher.dispatch(output)
        self._processed_count += 1

    def _resolve_ticker(self, entry: Dict) -> Optional[str]:
        if not self.ticker_lookup:
            return None
        cik = entry.get("cik") or entry.get("feed_entry", {}).get("cik")
        return self.ticker_lookup.lookup(cik)

    def _primary_document_url(self, documents_payload: Dict) -> Optional[str]:
        docs = documents_payload.get("documents") or []
        if not docs:
            return None
        for doc in docs:
            url = doc.get("url")
            if url:
                return url
        return None

    def _build_links(self, doc_url: Optional[str], ticker: Optional[str]) -> str:
        links = []
        sec_url = doc_url or 'https://www.sec.gov/edgar/browse/'
        links.append(f'[SEC EDGAR]({sec_url})')

        query = ticker.strip() if ticker else ''
        if query:
            twitter_url = f'https://x.com/search?q={quote_plus(query)}&src=typed_query'
            stocktwits_url = f'https://stocktwits.com/symbol/{quote_plus(query)}'
            links.append(f'[Twitter]({twitter_url})')
            links.append(f'[Stocktwits]({stocktwits_url})')

        return ' | '.join(links)


PLACEHOLDER_TOKENS = {"contact@example.com", "name@example.com"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll the SEC latest filings feed and parse each filing.")
    parser.add_argument(
        "--user-agent",
        "-u",
        default=os.environ.get("SEC_USER_AGENT"),
        help="SEC-compliant User-Agent string (or set SEC_USER_AGENT).",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=DEFAULT_POLL_INTERVAL,
        help="Feed polling interval in seconds (default: 7).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=40,
        help="How many entries to request from the feed per poll.",
    )
    parser.add_argument(
        "--doc-delay",
        type=float,
        default=DEFAULT_DOC_DELAY,
        help="Delay between filing downloads to stay within SEC guidance.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Emit compact JSON (one line per processed filing).",
    )
    parser.add_argument(
        "--owner",
        default="exclude",
        help="Value for the 'owner' feed query parameter (default: exclude).",
    )
    parser.add_argument(
        "--feed-url",
        help="Override the default feed URL entirely (advanced).",
    )
    parser.add_argument(
        "--discord",
        action="store_true",
        help="Send each processed filing to the Discord webhook(s) defined in the .env file.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="When used with --discord, use DISCORD_WEBHOOK_TEST_URL instead of DISCORD_WEBHOOK_URL.",
    )
    parser.add_argument(
        "--discord-template",
        default=str(REPO_ROOT / "discord_webhook_object.json"),
        help="Path to the Discord webhook JSON template (default: discord_webhook_object.json).",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        help="Process at most this many filings, then exit.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    user_agent = args.user_agent
    if not user_agent:
        raise SystemExit("Provide a SEC-compliant user agent via --user-agent or SEC_USER_AGENT.")
    if any(token in user_agent for token in PLACEHOLDER_TOKENS):
        raise SystemExit(
            "Please supply a real SEC-compliant User-Agent (include contact info) instead of the placeholder."
        )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    feed_url = args.feed_url or build_feed_url(count=args.count, owner=args.owner)
    ticker_lookup = CompanyTickerLookup(user_agent=user_agent)
    dispatcher: Optional[DiscordDispatcher] = None
    if args.discord:
        template_path = Path(args.discord_template).expanduser()
        if not template_path.is_file():
            raise SystemExit(f"Discord template not found: {template_path}")
        targets = load_discord_targets(use_test=args.test)
        if not targets:
            which = "DISCORD_WEBHOOK_TEST_URL" if args.test else "DISCORD_WEBHOOK_URL"
            raise SystemExit(f"No webhooks configured for {which}. Check your environment or .env file.")
        dispatcher = DiscordDispatcher(template_path, targets, user_agent=user_agent)

    poller = LatestFilingsPoller(
        feed_url=feed_url,
        user_agent=user_agent,
        poll_interval=args.poll,
        document_delay=args.doc_delay,
        compact=args.compact,
        ticker_lookup=ticker_lookup,
        dispatcher=dispatcher,
        max_results=args.max_results,
    )
    set_user_agent(user_agent)

    try:
        poller.run_forever()
    except KeyboardInterrupt:
        logging.getLogger("sec_feed.poller").info("Shutting down")


if __name__ == "__main__":
    main()
