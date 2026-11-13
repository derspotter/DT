"""LLM attempts: rate-limit waits per attempt, late results logged and charged (PY-44, PY-45),
and get_bib_pages' exit code after a failed PDF (PY-46)."""
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dl_lit import get_bib_pages
from dl_lit import llm_common
from dl_lit.llm_common import run_with_timeout


def test_before_attempt_runs_for_every_attempt():
    calls = []
    release = threading.Event()
    try:
        with pytest.raises(TimeoutError):
            run_with_timeout(
                lambda: release.wait(5),
                0.05,
                retries=2,
                before_attempt=lambda: calls.append(1),
            )
    finally:
        release.set()  # finish the abandoned attempts inside this test
        time.sleep(0.1)
    assert len(calls) == 3


def test_late_result_is_logged_and_its_tokens_charged(monkeypatch, capsys):
    charged = []
    monkeypatch.setattr(
        llm_common, "wait_rate_limit", lambda service, units=1: charged.append((service, units))
    )
    resp = SimpleNamespace(usage_metadata=SimpleNamespace(total_token_count=42))
    seq = iter([0.3, 0.0])  # first attempt sleeps past the timeout, second answers at once

    def call():
        time.sleep(next(seq))
        return resp

    assert run_with_timeout(call, 0.1, retries=1, token_service="gemini_tokens") is resp
    time.sleep(0.4)  # let the abandoned attempt finish
    err = capsys.readouterr().err
    assert "attempt 1 finished" in err and "tokens=42" in err
    assert "after its timeout and was discarded" in err
    assert charged == [("gemini_tokens", 42)]  # the late call only; the caller charges the returned one


def test_late_error_is_logged_not_raised(capsys, monkeypatch):
    hooked = []
    monkeypatch.setattr(threading, "excepthook", lambda args: hooked.append(args))

    def call():
        time.sleep(0.3)
        raise ValueError("late")

    with pytest.raises(TimeoutError):
        run_with_timeout(call, 0.1, retries=0)
    time.sleep(0.4)
    err = capsys.readouterr().err
    assert "error=ValueError: late" in err
    assert hooked == []


def test_result_on_time_is_not_late(capsys):
    assert run_with_timeout(lambda: 7, 1.0) == 7
    time.sleep(0.05)
    assert "[WARNING]" not in capsys.readouterr().err


def _dummy_pdf(tmp_path):
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    return pdf


def test_get_bib_pages_returns_1_when_a_pdf_failed(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(get_bib_pages, "api_client", object())

    def _boom(*_a, **_kw):
        raise RuntimeError("timeout")

    monkeypatch.setattr(get_bib_pages, "_process_pdf", _boom)
    pdf = _dummy_pdf(tmp_path)
    code = get_bib_pages.main(["--input-pdf", str(pdf), "--output-dir", str(tmp_path / "o")])
    assert code == 1
    assert "1 of 1 PDF(s) failed" in capsys.readouterr().out


def test_get_bib_pages_returns_0_on_success(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(get_bib_pages, "api_client", object())
    monkeypatch.setattr(get_bib_pages, "_process_pdf", lambda *_a, **_kw: ["p1.pdf"])
    pdf = _dummy_pdf(tmp_path)
    code = get_bib_pages.main(["--input-pdf", str(pdf), "--output-dir", str(tmp_path / "o")])
    assert code == 0
    assert "0 of 1 PDF(s) failed" in capsys.readouterr().out


def test_section_detection_waits_per_attempt(monkeypatch):
    waits = []
    monkeypatch.setattr(
        get_bib_pages, "wait_rate_limit", lambda service, units=1: waits.append(service)
    )
    response = MagicMock()
    response.text = '{"reference_sections": [{"start_page": 1, "end_page": 2}]}'
    behaviors = [llm_common._AttemptTimedOut(), llm_common._AttemptTimedOut(), response]

    def _fake_run_attempt(_call, _timeout_seconds, **_kw):
        behavior = behaviors.pop(0)
        if isinstance(behavior, BaseException):
            raise behavior
        return behavior

    monkeypatch.setattr(llm_common, "_run_attempt", _fake_run_attempt)
    monkeypatch.setattr(llm_common.time, "sleep", lambda _s: None)
    monkeypatch.setattr(get_bib_pages, "GET_BIB_TIMEOUT_RETRIES", 2)
    monkeypatch.setattr(get_bib_pages, "api_client", MagicMock())
    monkeypatch.setattr(get_bib_pages, "extract_total_tokens", lambda _r: None)

    sections, _meta = get_bib_pages._find_reference_section_pages(
        uploaded_pdf=object(),
        total_physical_pages=20,
        timeout_seconds=15,
        include_source_metadata=False,
    )

    assert sections == [{"start_page": 1, "end_page": 2}]
    assert waits.count("gemini") == 3
    assert waits.count("gemini_daily") == 3
