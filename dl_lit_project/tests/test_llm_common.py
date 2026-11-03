"""Tests for dl_lit.llm_common: the shared Gemini timeout/retry helper and LLM helpers.

The location-label and source-metadata cases were pinned against both former
copies (APIscraper_v2 and get_bib_pages) before they were unified; both agreed
on every case below.
"""
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from dl_lit import llm_common


# --- run_with_timeout -------------------------------------------------------

def test_run_with_timeout_does_not_wait_for_hung_call():
    start = time.monotonic()
    with pytest.raises(TimeoutError) as excinfo:
        llm_common.run_with_timeout(
            lambda: time.sleep(1.5),
            0.2,
            retries=1,
            backoff_seconds=0,
        )
    elapsed = time.monotonic() - start
    print(f"run_with_timeout elapsed: {elapsed:.3f}s")
    # 2 attempts x 0.2 s; waiting on a hung thread would take >= 1.5 s.
    assert elapsed < 1.0
    assert "after 2 attempt(s)" in str(excinfo.value)


def test_run_with_timeout_returns_value_on_second_attempt():
    calls = []

    def call():
        calls.append(1)
        if len(calls) == 1:
            time.sleep(0.6)
            return "late"
        return "ok"

    sleeps = []
    result = llm_common.run_with_timeout(
        call, 0.2, retries=1, backoff_seconds=0.25, label="test call", sleep=sleeps.append
    )
    assert result == "ok"
    assert len(calls) == 2
    assert sleeps == [0.25]


def test_run_with_timeout_backoff_grows_per_attempt_and_no_sleep_when_zero():
    sleeps = []
    with pytest.raises(TimeoutError):
        llm_common.run_with_timeout(
            lambda: time.sleep(0.5), 0.05, retries=2, backoff_seconds=1.5, sleep=sleeps.append
        )
    assert sleeps == [1.5, 3.0]

    sleeps.clear()
    with pytest.raises(TimeoutError):
        llm_common.run_with_timeout(
            lambda: time.sleep(0.5), 0.05, retries=1, backoff_seconds=0, sleep=sleeps.append
        )
    assert sleeps == []


def test_run_with_timeout_no_retries_and_errors_propagate():
    assert llm_common.run_with_timeout(lambda: 42, 1) == 42

    def boom():
        raise ValueError("bad")

    with pytest.raises(ValueError, match="bad"):
        llm_common.run_with_timeout(boom, 1, retries=3, sleep=lambda s: None)


def test_run_with_timeout_message_names_label(capsys):
    with pytest.raises(TimeoutError):
        llm_common.run_with_timeout(
            lambda: time.sleep(0.5), 0.05, retries=1, label="my label", sleep=lambda s: None
        )
    out = capsys.readouterr().out
    assert "[WARNING] my label timed out after 0.05s (attempt 1/2)" in out


def test_hung_call_does_not_keep_the_process_alive():
    """The abandoned attempt must not be joined at interpreter exit."""
    script = textwrap.dedent(
        """
        import time
        from dl_lit.llm_common import run_with_timeout
        try:
            run_with_timeout(lambda: time.sleep(3), 0.2)
        except TimeoutError:
            pass
        """
    )
    project_dir = Path(__file__).resolve().parent.parent
    start = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_dir,
        capture_output=True,
        text=True,
        timeout=5,
    )
    elapsed = time.perf_counter() - start
    print(f"subprocess exit after: {elapsed:.3f}s")
    assert proc.returncode == 0, proc.stderr
    assert elapsed < 1.5


def test_timeout_error_raised_by_call_propagates_unchanged():
    calls = []
    original = TimeoutError("socket read timed out")

    def call():
        calls.append(1)
        raise original

    sleeps = []
    with pytest.raises(TimeoutError) as excinfo:
        llm_common.run_with_timeout(call, 1, retries=2, backoff_seconds=1, sleep=sleeps.append)
    assert excinfo.value is original
    assert len(calls) == 1
    assert sleeps == []


def test_returns_none_result_and_base_exceptions_propagate():
    assert llm_common.run_with_timeout(lambda: None, 1) is None

    def interrupted():
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        llm_common.run_with_timeout(interrupted, 1, retries=1, sleep=lambda s: None)


# --- extract_total_tokens ---------------------------------------------------

@pytest.mark.parametrize(
    "usage, expected",
    [
        (None, None),
        (SimpleNamespace(total_token_count=120), 120),
        (SimpleNamespace(total_token_count=12.9), 12),
        (SimpleNamespace(total_token_count=0, total_tokens=7), 7),
        (SimpleNamespace(total_token_count=None, total_tokens=" 33 "), 33),
        (SimpleNamespace(total_token_count="abc"), None),
        (SimpleNamespace(total_token_count=-5), None),
    ],
)
def test_extract_total_tokens(usage, expected):
    response = SimpleNamespace(usage_metadata=usage)
    assert llm_common.extract_total_tokens(response) == expected


def test_extract_total_tokens_without_usage_attribute():
    assert llm_common.extract_total_tokens(object()) is None
    assert llm_common.extract_total_tokens(None) is None


# --- looks_like_location_label ---------------------------------------------

@pytest.mark.parametrize(
    "value, expected",
    [
        (None, False),
        ("", False),
        ("   ", False),
        (5, False),
        (["Berlin"], False),
        ("Berlin", False),
        ("Springer", False),
        ("Oxford University Press", False),
        ("Journal of Politics", False),
        ("Cambridge, MA", True),
        ("New York, NY", True),
        ("London,  UK", True),
        ("Thousand Oaks, CA ", True),
        ("St. Louis, MO", True),
        ("O'Fallon, IL", True),
        ("cambridge, MA", False),
        ("Frankfurt am Main, Germany", False),
        ("Berlin, Germany, Europe", False),
        ("A" * 81, False),
    ],
)
def test_looks_like_location_label(value, expected):
    assert llm_common.looks_like_location_label(value) is expected


# --- normalize_source_metadata ---------------------------------------------

@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        ("", None),
        ([], None),
        ({}, None),
        (
            {
                "title": "  T  ",
                "authors": [" A ", "", None, 3],
                "year": 2021.7,
                "doi": " 10.1/x ",
                "publisher": " Springer ",
                "source": " Cambridge, MA ",
                "extra": 1,
            },
            {"title": "T", "authors": ["A", "None", "3"], "year": 2021, "doi": "10.1/x", "publisher": "Springer"},
        ),
        ({"source": "Journal of X", "year": " 2020 "}, {"year": "2020", "source": "Journal of X"}),
        ({"year": "  ", "title": "", "authors": "Smith"}, None),
        ({"year": True, "source": "Berlin"}, {"year": 1, "source": "Berlin"}),
        ({"authors": [], "doi": 5}, None),
        ({"source": "Springer", "location": "Berlin"}, {"source": "Springer"}),
    ],
)
def test_normalize_source_metadata(value, expected):
    assert llm_common.normalize_source_metadata(value) == expected


# --- wait_rate_limit -------------------------------------------------------

class _RecordingLimiter:
    def __init__(self):
        self.calls = []

    def wait_if_needed(self, service, units=1):
        self.calls.append((service, units))
        return True


def test_wait_rate_limit_uses_current_global_limiter(monkeypatch):
    limiter = _RecordingLimiter()
    monkeypatch.setattr(llm_common, "get_global_rate_limiter", lambda: limiter)
    llm_common.wait_rate_limit("gemini")
    llm_common.wait_rate_limit("gemini_tokens", units=250)
    assert limiter.calls == [("gemini", 1), ("gemini_tokens", 250)]


def test_wait_rate_limit_skips_none_and_swallows_errors(monkeypatch):
    monkeypatch.setattr(llm_common, "get_global_rate_limiter", lambda: None)
    assert llm_common.wait_rate_limit("gemini") is None

    class _Broken:
        def wait_if_needed(self, service, units=1):
            raise RuntimeError("limiter down")

    monkeypatch.setattr(llm_common, "get_global_rate_limiter", lambda: _Broken())
    assert llm_common.wait_rate_limit("gemini") is None

    def _raising():
        raise RuntimeError("cannot build limiter")

    monkeypatch.setattr(llm_common, "get_global_rate_limiter", _raising)
    assert llm_common.wait_rate_limit("gemini") is None


# --- get_bib_pages regression: the retry no longer waits for the hung call --

def test_get_bib_pages_timeout_does_not_wait_for_hung_call(monkeypatch):
    from unittest.mock import MagicMock

    from dl_lit import get_bib_pages

    client = MagicMock()
    client.models.generate_content.side_effect = lambda **kwargs: time.sleep(1.5)
    monkeypatch.setattr(get_bib_pages, "api_client", client)
    monkeypatch.setattr(get_bib_pages, "GET_BIB_TIMEOUT_RETRIES", 1)
    monkeypatch.setattr(get_bib_pages, "GET_BIB_RETRY_BACKOFF_SECONDS", 0.0)
    monkeypatch.setattr(get_bib_pages, "wait_rate_limit", lambda *args, **kwargs: None)

    start = time.monotonic()
    with pytest.raises(TimeoutError):
        get_bib_pages._find_reference_section_pages(
            uploaded_pdf=object(), total_physical_pages=5, timeout_seconds=0.2
        )
    elapsed = time.monotonic() - start
    print(f"get_bib_pages elapsed: {elapsed:.3f}s")
    assert elapsed < 1.0
    assert client.models.generate_content.call_count == 2
