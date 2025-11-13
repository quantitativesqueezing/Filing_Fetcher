import os
import re
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Dict, List, Iterable, Optional

DEFAULT_USER_AGENT = os.environ.get("SEC_USER_AGENT", "Filing_Fetcher/0.1 (contact@example.com)")
UA = {"User-Agent": DEFAULT_USER_AGENT}

def set_user_agent(user_agent: Optional[str]) -> None:
    """Override the SEC User-Agent header at runtime."""
    if not user_agent:
        return
    UA["User-Agent"] = user_agent.strip()
XSL_XML_RE = re.compile(r"/xsl[^/]+/.*\.xml$", re.IGNORECASE)

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def find_documents_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    t = soup.select_one("table#documents")
    if t:
        return t
    hdr = soup.find(lambda tag: tag.name in ("h2", "h3", "h4") and "Document Format Files" in tag.get_text(strip=True))
    if hdr:
        t = hdr.find_next("table")
        if t:
            return t
    for cand in soup.find_all("table"):
        tr0 = cand.find("tr")
        if not tr0:
            continue
        cols = [c.get_text(strip=True).lower() for c in tr0.find_all(["th", "td"])]
        if any("document" in c for c in cols) and any("type" in c for c in cols):
            return cand
    return None

def map_header_indices(table: BeautifulSoup) -> Dict[str, Optional[int]]:
    header_cells = table.find("tr").find_all(["th", "td"])
    labels = [c.get_text(strip=True).lower() for c in header_cells]
    def find(label: str) -> Optional[int]:
        for i, lab in enumerate(labels):
            if label in lab:
                return i
        return None
    return {
        "seq":  find("seq"),
        "desc": find("description"),
        "doc":  find("document"),
        "type": find("type"),
        "size": find("size"),
    }

def accession_from_index_url(index_url: str) -> Optional[str]:
    m = re.search(r"/(\d{10}-\d{2}-\d{6})-index\.htm$", index_url)
    return m.group(1) if m else None

def render_hint_for_href(href_path: str) -> Optional[Dict]:
    if not href_path:
        return None
    hint: Dict[str, str] = {}
    if XSL_XML_RE.search(href_path):
        hint["xml_with_xsl"] = True
        parts = href_path.strip("/").split("/")
        if len(parts) >= 1:
            hint["xsl_folder"] = parts[-2] if len(parts) >= 2 else parts[-1]
        if href_path.lower().endswith(".xml"):
            hint["suggested_html_twin"] = href_path[:-4] + ".html"
    return hint or None

def parse_documents_table(table: BeautifulSoup, index_url: str) -> List[Dict]:
    idx = map_header_indices(table)
    docs: List[Dict] = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        anchor_cell = tds[idx["doc"]] if (idx["doc"] is not None and idx["doc"] < len(tds)) else None
        a = anchor_cell.find("a") if anchor_cell else tr.find("a")
        href_raw = a.get("href") if a and a.has_attr("href") else None
        url_abs = urljoin("https://www.sec.gov", href_raw) if href_raw else None
        seq  = tds[idx["seq"]].get_text(strip=True) if idx["seq"] is not None and idx["seq"] < len(tds) else None
        desc = tds[idx["desc"]].get_text(strip=True) if idx["desc"] is not None and idx["desc"] < len(tds) else None
        doc_text = a.get_text(strip=True) if a else (
            tds[idx["doc"]].get_text(strip=True) if idx["doc"] is not None and idx["doc"] < len(tds) else None
        )
        ftype = tds[idx["type"]].get_text(strip=True) if idx["type"] is not None and idx["type"] < len(tds) else None
        size  = tds[idx["size"]].get_text(strip=True) if idx["size"] is not None and idx["size"] < len(tds) else None
        hint = render_hint_for_href(href_raw or "")
        docs.append({
            "seq": seq,
            "description": desc,
            "document_text": doc_text,
            "href_raw": href_raw,
            "url": url_abs,
            "type": ftype,
            "size": size,
            "render_hint": hint
        })
    return docs

def find_complete_submission_link(soup: BeautifulSoup) -> Optional[Dict]:
    link = soup.find("a", string=re.compile(r"Complete submission text file", re.I))
    if link and link.has_attr("href"):
        href = link["href"]
        return {
            "label": "Complete submission text file",
            "href_raw": href,
            "url": urljoin("https://www.sec.gov", href)
        }
    return None

def fetch_index_documents(index_url: str) -> Dict:
    soup = get_soup(index_url)
    table = find_documents_table(soup)
    docs = parse_documents_table(table, index_url) if table else []
    complete = find_complete_submission_link(soup)
    return {
        "accession_number": accession_from_index_url(index_url),
        "index_url": index_url,
        "documents": docs,
        "complete_submission": complete
    }

def to_ndjson(docs_payload: Dict) -> Iterable[str]:
    acc = docs_payload.get("accession_number")
    idx_url = docs_payload.get("index_url")
    ticker = docs_payload.get("ticker")
    for d in docs_payload.get("documents", []):
        row = {"accession_number": acc, "index_url": idx_url}
        if ticker is not None:
            row["ticker"] = ticker
        row.update(d)
        yield json.dumps(row, separators=(",", ":"))
