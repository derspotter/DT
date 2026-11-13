"""Shared LLM helpers for the extraction modules (APIscraper_v2, get_bib_pages).

- ``run_with_timeout``: run a blocking LLM call with a per-attempt timeout and retries.
  Each attempt runs on its own daemon thread that is abandoned on timeout, so a hung
  call never delays the retry, the final ``TimeoutError`` or interpreter exit.
  ``before_attempt`` runs before every attempt (rate-limit waits); an attempt that
  finishes after its timeout is logged and its tokens charged to ``token_service``.
- ``extract_total_tokens``: total token count from a Gemini response's usage metadata.
- ``looks_like_location_label`` / ``normalize_source_metadata``: clean the
  ``source_metadata`` block the model returns (a place such as "Cambridge, MA" is
  not a source title).
- ``wait_rate_limit``: best-effort wait on the global rate limiter.
"""
from __future__ import annotations

import re
import sys
import threading
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


class _AttemptTimedOut(Exception):
    """One attempt exceeded its timeout (kept apart from a TimeoutError raised by the call)."""


def _run_attempt(call, timeout_seconds, *, on_late=None):
    """Run ``call()`` on a daemon thread; raise ``_AttemptTimedOut`` if it outlives the timeout.

    A ``concurrent.futures`` executor is not used on purpose: its worker threads are
    joined at interpreter exit, so a hung call would keep the process alive. The
    abandoned daemon thread is left to finish (or die with the process). If it finishes
    after the timeout, ``on_late(ok, value, late_seconds)`` runs on that thread, so a late
    result or error is reported instead of vanishing. One lock decides whether an outcome
    is late: the target appends its outcome under it, the caller marks the attempt
    abandoned under it when there is no outcome yet.
    """
    outcome = []
    lock = threading.Lock()
    state = {"abandoned": False}
    deadline = time.monotonic() + timeout_seconds

    def _target():
        try:
            result = (True, call())
        except BaseException as exc:  # re-raised in the caller's thread
            result = (False, exc)
        with lock:
            outcome.append(result)
            late = state["abandoned"]
        if late and on_late is not None:
            on_late(result[0], result[1], max(0.0, time.monotonic() - deadline))

    thread = threading.Thread(target=_target, name="llm-call", daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    with lock:
        if not outcome:
            state["abandoned"] = True
            raise _AttemptTimedOut()
        ok, value = outcome[0]
    if ok:
        return value
    raise value


def _late_handler(label, attempt, token_service):
    """The default ``on_late`` for attempt ``attempt``: log one line, charge late tokens."""

    def _on_late(ok, value, late_seconds):
        if ok:
            tokens = extract_total_tokens(value)
            detail = f"tokens={tokens}"
        else:
            tokens = None
            detail = f"error={type(value).__name__}: {value}"
        print(
            f"[WARNING] {label} attempt {attempt} finished {late_seconds:.1f}s after its "
            f"timeout and was discarded ({detail})",
            file=sys.stderr,
            flush=True,
        )
        if ok and token_service and tokens:
            wait_rate_limit(token_service, units=tokens)

    return _on_late


def run_with_timeout(
    call,
    timeout_seconds,
    *,
    retries=0,
    backoff_seconds=0.0,
    label="LLM request",
    sleep=time.sleep,
    before_attempt=None,
    token_service=None,
):
    """Return ``call()``; retry ``retries`` times on timeout, then raise ``TimeoutError``.

    The delay before retry ``n`` is ``backoff_seconds * n``. Exceptions raised by
    ``call`` itself (including its own ``TimeoutError``) propagate unchanged and are
    not retried; only the per-attempt timeout enforced here is retried.

    ``before_attempt()`` runs before every attempt, retries included (the rate-limit
    waits). An attempt that finishes after its timeout is logged to stderr; when
    ``token_service`` is set, its tokens are charged there. The tokens of the attempt
    that returns are the caller's to charge.
    """
    max_attempts = max(1, int(retries) + 1)
    for attempt in range(1, max_attempts + 1):
        if before_attempt is not None:
            before_attempt()
        try:
            return _run_attempt(
                call,
                timeout_seconds,
                on_late=_late_handler(label, attempt, token_service),
            )
        except _AttemptTimedOut as exc:
            if attempt >= max_attempts:
                raise TimeoutError(
                    f"{label} timed out after {timeout_seconds}s after {max_attempts} attempt(s)"
                ) from exc
            delay = max(0.0, float(backoff_seconds)) * attempt
            print(
                f"[WARNING] {label} timed out after {timeout_seconds}s "
                f"(attempt {attempt}/{max_attempts}). Retrying in {delay:.1f}s...",
                flush=True,
            )
            if delay > 0:
                sleep(delay)


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
