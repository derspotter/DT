from unittest.mock import MagicMock

from dl_lit import APIscraper_v2 as scraper
from dl_lit import get_bib_pages
from dl_lit import llm_common


def _fake_attempts(behaviors, seen_timeouts):
    """Stand-in for llm_common._run_attempt: each attempt pops one scripted outcome."""

    def _run_attempt(_call, timeout_seconds):
        seen_timeouts.append(timeout_seconds)
        behavior = behaviors.pop(0)
        if isinstance(behavior, BaseException):
            raise behavior
        return behavior

    return _run_attempt


def test_apiscraper_run_gemini_request_retries_timeout(monkeypatch):
    response = MagicMock()
    behaviors = [llm_common._AttemptTimedOut(), response]
    seen_timeouts = []

    monkeypatch.setattr(llm_common, "_run_attempt", _fake_attempts(behaviors, seen_timeouts))
    sleeps = []
    monkeypatch.setattr(llm_common.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = scraper._run_gemini_request(
        lambda: response,
        timeout_seconds=12,
        timeout_retries=1,
        retry_backoff_seconds=0.25,
        action_label="test call",
    )

    assert result is response
    assert sleeps == [0.25]
    assert seen_timeouts == [12, 12]


def test_process_single_pdf_chunked_retries_only_timeout_failures(monkeypatch):
    monkeypatch.setattr(scraper, "INLINE_CHUNK_FAILURE_RETRIES", 1)
    monkeypatch.setattr(scraper, "INLINE_CHUNK_RETRY_BACKOFF_SECONDS", 0.0)
    monkeypatch.setattr(scraper.time, "sleep", lambda _seconds: None)

    chunk_paths = ["/tmp/chunk1.pdf", "/tmp/chunk2.pdf"]
    monkeypatch.setattr(scraper, "_split_pdf_into_chunks", lambda path, chunk_pages: ("/tmp/chunks", chunk_paths))
    monkeypatch.setattr(scraper.os.path, "exists", lambda path: False)

    call_counts = {path: 0 for path in chunk_paths}

    def _fake_process_single_pdf(
        pdf_path,
        db_manager,
        summary_dict,
        summary_lock,
        db_write_lock=None,
        ingest_source=None,
        corpus_id=None,
        extract_mode="inline",
        source_pdf_override=None,
        uploaded_file_name=None,
        seed_only=False,
        record_summary=True,
    ):
        call_counts[pdf_path] += 1
        if pdf_path == "/tmp/chunk1.pdf" and call_counts[pdf_path] == 1:
            return False, "API extraction failed: Gemini request timed out after 120s"
        return True, ""

    monkeypatch.setattr(scraper, "process_single_pdf", _fake_process_single_pdf)

    summary = {"processed_files": 0, "successful_files": 0, "failed_files": 0, "failures": []}
    scraper.process_single_pdf_chunked(
        "/tmp/original.pdf",
        db_manager=object(),
        summary_dict=summary,
        summary_lock=scraper.Lock(),
        db_write_lock=scraper.Lock(),
        ingest_source="run-1",
        corpus_id=1,
        extract_mode="inline",
        chunk_pages=10,
        max_workers=1,
        seed_only=True,
    )

    assert call_counts["/tmp/chunk1.pdf"] == 2
    assert call_counts["/tmp/chunk2.pdf"] == 1
    assert summary["processed_files"] == 2
    assert summary["successful_files"] == 2
    assert summary["failed_files"] == 0
    assert summary["failures"] == []


def test_get_bib_pages_retries_timeout(monkeypatch):
    response = MagicMock()
    response.text = '{"reference_sections": [{"start_page": 1, "end_page": 2}]}'
    behaviors = [llm_common._AttemptTimedOut(), response]
    seen_timeouts = []

    monkeypatch.setattr(llm_common, "_run_attempt", _fake_attempts(behaviors, seen_timeouts))
    monkeypatch.setattr(get_bib_pages, "GET_BIB_TIMEOUT_RETRIES", 1)
    sleeps = []
    monkeypatch.setattr(llm_common.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(get_bib_pages, "api_client", MagicMock())
    monkeypatch.setattr(get_bib_pages, "wait_rate_limit", lambda *args, **kwargs: None)
    monkeypatch.setattr(get_bib_pages, "extract_total_tokens", lambda response: None)

    sections, source_metadata = get_bib_pages._find_reference_section_pages(
        uploaded_pdf=object(),
        total_physical_pages=20,
        timeout_seconds=15,
        include_source_metadata=False,
    )

    assert sections == [{"start_page": 1, "end_page": 2}]
    assert source_metadata is None
    assert sleeps == [get_bib_pages.GET_BIB_RETRY_BACKOFF_SECONDS]
    assert seen_timeouts == [15, 15]