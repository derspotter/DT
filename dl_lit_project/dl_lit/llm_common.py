"""Shared LLM helpers for the extraction modules (APIscraper_v2, get_bib_pages).

- ``run_with_timeout``: run a blocking LLM call with a per-attempt timeout and retries.
  Each attempt gets its own single-worker executor that is shut down with
  ``wait=False`` so a hung call never delays the retry or the final ``TimeoutError``.
- ``extract_total_tokens``: total token count from a Gemini response's usage metadata.
- ``looks_like_location_label`` / ``normalize_source_metadata``: clean the
  ``source_metadata`` block the model returns (a place such as "Cambridge, MA" is
  not a source title).
- ``wait_rate_limit``: best-effort wait on the global rate limiter.
"""
from __future__ import annotations

import concurrent.futures
import re
import time

from dl_lit.utils import get_global_rate_limiter

__all__ = [
    "LOCATION_LIKE_SOURCE_RE",
    "extract_total_tokens",
    "looks_like_location_label",
    "normalize_source_metadata",
    "run_with_timeout",
    "wait_rate_limit",
]


LOCATION_LIKE_SOURCE_RE = re.compile(
    r"^[A-Z][A-Za-z.'-]+(?: [A-Z][A-Za-z.'-]+){0,3},\s*(?:[A-Z]{2}|[A-Z][A-Za-z.'-]+(?: [A-Z][A-Za-z.'-]+){0,3})$"
)


def run_with_timeout(
    call,
    timeout_seconds,
    *,
    retries=0,
    backoff_seconds=0.0,
    label="LLM request",
    sleep=time.sleep,
):
    """Return ``call()``; retry ``retries`` times on timeout, then raise ``TimeoutError``.

    The delay before retry ``n`` is ``backoff_seconds * n``. Exceptions raised by
    ``call`` itself propagate unchanged and are not retried.
    """
    max_attempts = max(1, int(retries) + 1)
    for attempt in range(1, max_attempts + 1):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(call)
        try:
            return future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            if attempt >= max_attempts:
                raise TimeoutError(
                    f"Gemini request timed out after {timeout_seconds}s after {max_attempts} attempt(s)"
                ) from exc
            delay = max(0.0, float(backoff_seconds)) * attempt
            print(
                f"[WARNING] {label} timed out after {timeout_seconds}s "
                f"(attempt {attempt}/{max_attempts}). Retrying in {delay:.1f}s...",
                flush=True,
            )
            if delay > 0:
                sleep(delay)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)


def extract_total_tokens(response) -> int | None:
    try:
        usage = getattr(response, "usage_metadata", None)
        if usage is None:
            return None
        for key in ("total_token_count", "total_tokens"):
            value = getattr(usage, key, None)
            if isinstance(value, (int, float)) and value > 0:
                return int(value)
            if isinstance(value, str) and value.strip().isdigit():
                return int(value.strip())
    except Exception:
        return None
    return None


def looks_like_location_label(value) -> bool:
    if not isinstance(value, str):
        return False
    candidate = " ".join(value.strip().split())
    if not candidate or len(candidate) > 80:
        return False
    return bool(LOCATION_LIKE_SOURCE_RE.fullmatch(candidate))


def normalize_source_metadata(value) -> dict | None:
    if not isinstance(value, dict):
        return None
    metadata: dict = {}
    title = value.get("title")
    if isinstance(title, str) and title.strip():
        metadata["title"] = title.strip()
    authors = value.get("authors")
    if isinstance(authors, list):
        cleaned = [str(a).strip() for a in authors if str(a).strip()]
        if cleaned:
            metadata["authors"] = cleaned
    year = value.get("year")
    if isinstance(year, (int, float)):
        metadata["year"] = int(year)
    elif isinstance(year, str):
        cleaned_year = year.strip()
        if cleaned_year:
            metadata["year"] = cleaned_year
    doi = value.get("doi")
    if isinstance(doi, str) and doi.strip():
        metadata["doi"] = doi.strip()
    publisher = value.get("publisher")
    if isinstance(publisher, str) and publisher.strip():
        metadata["publisher"] = publisher.strip()
    source = value.get("source")
    if isinstance(source, str) and source.strip():
        cleaned_source = source.strip()
        if not looks_like_location_label(cleaned_source):
            metadata["source"] = cleaned_source
    return metadata or None


def wait_rate_limit(service, units=1) -> None:
    """Best-effort rate limiting; throttling problems never fail an extraction."""
    try:
        limiter = get_global_rate_limiter()
        if limiter:
            limiter.wait_if_needed(service, units=units)
    except Exception:
        return
