"""
Utility helpers for text normalization and keyword scoring.
"""

from __future__ import annotations

import html
import re
from typing import Iterable, List, Tuple

SCRIPT_STYLE_RE = re.compile(r"<(script|style).*?>.*?</\1>", re.IGNORECASE | re.DOTALL)
HTML_TAG_RE = re.compile(r"<[^>]+>")
SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9])")


def normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace characters."""
    return re.sub(r"\s+", " ", text).strip()


def html_to_text(content: str) -> str:
    """Remove basic markup and produce a plaintext representation."""
    stripped = SCRIPT_STYLE_RE.sub(" ", content)
    stripped = HTML_TAG_RE.sub(" ", stripped)
    return normalize_whitespace(html.unescape(stripped))


def split_sentences(text: str, limit: int | None = None) -> List[str]:
    """Split text into sentences with a simple rule-based approach."""
    sentences = SENTENCE_BOUNDARY_RE.split(text)
    sentences = [normalize_whitespace(sentence) for sentence in sentences if sentence.strip()]
    if limit is not None:
        return sentences[:limit]
    return sentences


def keyword_score(text: str, keywords: Iterable[Tuple[str, float]]) -> float:
    """
    Compute a weighted keyword score based on the number of matches.

    Parameters
    ----------
    text:
        Text to evaluate.
    keywords:
        Iterable of (phrase, weight).
    """
    lowered = text.lower()
    total = 0.0
    for phrase, weight in keywords:
        occurrences = lowered.count(phrase)
        if occurrences:
            total += weight * occurrences
    return total

