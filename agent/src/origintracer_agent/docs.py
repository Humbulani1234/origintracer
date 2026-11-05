"""Lightweight live documentation retrieval for product-chat questions."""

from __future__ import annotations

import asyncio
import html
import json
import logging
import math
import os
import re
import time
from collections import Counter
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx

logger = logging.getLogger(__name__)

DEFAULT_DOCS_BASE_URL = "https://origintracer.app/docs/"
DOCS_INDEX_TTL_S = 600
DOCS_INDEX_FAILURE_TTL_S = 60
DOCS_MAX_RESULTS = 4
DOCS_MAX_EXCERPT_CHARS = 1_800

_STOP_WORDS = {
    "a",
    "about",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "can",
    "could",
    "do",
    "does",
    "for",
    "from",
    "give",
    "how",
    "i",
    "in",
    "is",
    "it",
    "me",
    "of",
    "on",
    "or",
    "please",
    "tell",
    "that",
    "the",
    "their",
    "this",
    "to",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "with",
    "would",
}

_index_rows: list[dict[str, Any]] | None = None
_index_url: str | None = None
_index_loaded_at = 0.0
_index_cache_ttl_s = DOCS_INDEX_TTL_S
_index_lock = asyncio.Lock()


def _docs_base_url() -> str:
    return (
        os.getenv(
            "ORIGINTRACER_DOCS_URL", DEFAULT_DOCS_BASE_URL
        ).rstrip("/")
        + "/"
    )


def _tokens(value: str) -> list[str]:
    """Tokenize prose and technical identifiers without losing compounds."""
    camel_split = re.sub(
        r"(?<=[A-Z])(?=[A-Z][a-z])|(?<=[a-z0-9])(?=[A-Z])",
        " ",
        value,
    )
    raw_tokens = re.findall(
        r"[A-Za-z0-9]+(?:\+[A-Za-z0-9]+)+|[A-Za-z0-9]+(?:_[A-Za-z0-9]+)*",
        camel_split,
    )
    tokens: list[str] = []
    for raw in raw_tokens:
        normalized = raw.lower().replace("+", "_plus_")
        candidates = [normalized]
        if re.match(r"^n_plus_(?:one|1)(?:_|$)", normalized):
            candidates.append("n_plus_one")
        candidates.extend(normalized.split("_"))
        for token in candidates:
            if token in _STOP_WORDS or (
                len(token) == 1 and not token.isdigit()
            ):
                continue
            tokens.append(token)
    return tokens


def _plain_text(value: str) -> str:
    """Convert indexed HTML to text while retaining useful section boundaries."""
    text = re.sub(
        r"<(?:p|div|li|h[1-6]|tr|section|article|pre|blockquote)\b[^>]*>|"
        r"</(?:p|div|li|h[1-6]|tr|section|article|pre|blockquote)\s*>",
        "\n\n",
        value,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]*>", " ", text)
    text = html.unescape(text)
    lines = [
        re.sub(r"[ \t]+", " ", line).strip()
        for line in text.splitlines()
    ]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


async def _load_docs_index() -> list[dict[str, Any]]:
    """Fetch and cache the published MkDocs search index."""
    global _index_rows, _index_url, _index_loaded_at, _index_cache_ttl_s

    base_url = _docs_base_url()
    index_url = urljoin(base_url, "search/search_index.json")
    now = time.monotonic()
    if (
        _index_rows is not None
        and _index_url == index_url
        and now - _index_loaded_at < _index_cache_ttl_s
    ):
        return _index_rows

    async with _index_lock:
        now = time.monotonic()
        if (
            _index_rows is not None
            and _index_url == index_url
            and now - _index_loaded_at < _index_cache_ttl_s
        ):
            return _index_rows

        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(4.0, connect=1.5),
                follow_redirects=True,
                headers={
                    "User-Agent": "OriginTracer-Agent/0.1 docs retrieval"
                },
            ) as client:
                response = await client.get(index_url)
                response.raise_for_status()
                payload = response.json()
            rows = (
                payload.get("docs")
                if isinstance(payload, dict)
                else None
            )
            if not isinstance(rows, list):
                raise ValueError(
                    "documentation search index has no docs list"
                )
            _index_rows = [
                row for row in rows if isinstance(row, dict)
            ]
            _index_url = index_url
            _index_loaded_at = time.monotonic()
            _index_cache_ttl_s = DOCS_INDEX_TTL_S
        except (
            httpx.HTTPError,
            json.JSONDecodeError,
            ValueError,
        ) as exc:
            logger.info(
                "OriginTracer docs index unavailable: %s", exc
            )
            # Keep previously fetched rows for a short outage window; after a
            # first failure, cache the empty result briefly to avoid a request
            # to the docs host for every general-chat turn.
            if _index_rows is None or _index_url != index_url:
                _index_rows = []
            _index_url = index_url
            _index_loaded_at = time.monotonic()
            _index_cache_ttl_s = DOCS_INDEX_FAILURE_TTL_S
            return _index_rows

    return _index_rows or []


def _source_url(location: str, base_url: str) -> str | None:
    url = urljoin(base_url, location.lstrip("/"))
    base = urlparse(base_url)
    parsed = urlparse(url)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.netloc != base.netloc
    ):
        return None
    return url


def _excerpt(text: str, terms: set[str]) -> str:
    """Select the most query-relevant sentences instead of clipping the page head."""
    parts = [
        part.strip()
        for part in re.split(
            r"\n\s*\n|(?<=[.!?])\s+(?=[A-Z0-9#])",
            text,
        )
        if part.strip()
    ]
    if not parts:
        return ""

    ranked: list[tuple[int, int, str]] = []
    for index, part in enumerate(parts):
        counts = Counter(_tokens(part))
        overlap = terms.intersection(counts)
        if overlap:
            score = len(overlap) * 3 + sum(
                min(counts[token], 3) for token in overlap
            )
            ranked.append((score, -index, part))

    if not ranked:
        return text[:DOCS_MAX_EXCERPT_CHARS].strip()

    selected: list[str] = []
    used_chars = 0
    for _, _, part in sorted(ranked, reverse=True):
        if len(selected) >= 3:
            break
        if len(part) > DOCS_MAX_EXCERPT_CHARS:
            positions = [
                match.start()
                for term in terms
                if len(term) > 1
                for match in re.finditer(
                    re.escape(term), part, re.IGNORECASE
                )
            ]
            center = min(positions) if positions else 0
            start = max(0, center - DOCS_MAX_EXCERPT_CHARS // 3)
            end = min(len(part), start + DOCS_MAX_EXCERPT_CHARS)
            part = part[start:end].strip()
        if used_chars + len(part) > DOCS_MAX_EXCERPT_CHARS:
            continue
        selected.append(part)
        used_chars += len(part)

    if not selected:
        return ranked[0][2][:DOCS_MAX_EXCERPT_CHARS].strip()

    selected_parts = set(selected)
    ordered = [part for part in parts if part in selected_parts]
    return " … ".join(ordered)


def _rank_docs(
    rows: list[dict[str, Any]],
    query: str,
    limit: int,
    *,
    base_url: str,
) -> list[dict[str, str]]:
    """Rank indexed sections by query-term relevance and return excerpts."""
    query_terms = set(_tokens(query))
    if not query_terms:
        return []

    prepared: list[
        tuple[
            dict[str, Any], str, str, Counter[str], Counter[str]
        ]
    ] = []
    doc_frequency: Counter[str] = Counter()
    total_length = 0
    for row in rows:
        title = str(row.get("title", ""))
        body = _plain_text(str(row.get("text", "")))
        title_counts = Counter(_tokens(title))
        body_counts = Counter(_tokens(body))
        doc_frequency.update(
            set(title_counts) | set(body_counts)
        )
        total_length += sum(body_counts.values())
        prepared.append(
            (row, title, body, title_counts, body_counts)
        )

    doc_count = max(1, len(prepared))
    average_length = max(1.0, total_length / doc_count)
    ranked: list[tuple[float, dict[str, str]]] = []
    for row, title, body, title_counts, body_counts in prepared:
        length = max(1, sum(body_counts.values()))
        score = 0.0
        for term in query_terms:
            frequency = doc_frequency.get(term, 0)
            if frequency == 0:
                continue
            inverse_frequency = math.log(
                1
                + (doc_count - frequency + 0.5)
                / (frequency + 0.5)
            )
            title_weight = (
                min(title_counts.get(term, 0), 3) * 4.0
            )
            body_weight = min(body_counts.get(term, 0), 5) / (
                0.5 + 1.5 * length / average_length
            )
            score += inverse_frequency * (
                title_weight + body_weight
            )

        location = str(row.get("location", ""))
        source = _source_url(location, base_url)
        if score <= 0 or not source:
            continue
        ranked.append(
            (
                score,
                {
                    "title": title
                    or "OriginTracer documentation",
                    "url": source,
                    "excerpt": _excerpt(body, query_terms),
                },
            )
        )

    ranked.sort(key=lambda item: item[0], reverse=True)
    return [item for _, item in ranked[:limit]]


async def retrieve_documentation(
    query: str, *, limit: int = DOCS_MAX_RESULTS
) -> list[dict[str, str]]:
    """Return ranked live-doc excerpts with source URLs for a user query.

    An empty list means there were no matching sections or the remote index
    could not be fetched; callers can continue with their normal fallback.
    """
    if not _tokens(query):
        return []
    rows = await _load_docs_index()
    return _rank_docs(
        rows,
        query,
        max(1, min(limit, DOCS_MAX_RESULTS)),
        base_url=_docs_base_url(),
    )
