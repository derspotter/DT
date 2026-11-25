from pathlib import Path

import pytest

from dl_lit.scraper_lab import (
    build_artifact_record,
    build_eurlex_expert_query,
    raise_for_eurlex_response,
    safe_filename,
    validate_download_content,
)


def test_build_eurlex_query_escapes_text():
    query = build_eurlex_expert_query(
        {
            "fm_coded": "REG",
            "dd_year": "2024",
            "celex_id": "32024R1689",
            "lang": "EN",
            "text_query": "children's rights",
        }
    )

    assert "FM_CODED = REG" in query
    assert "DD_YEAR = 2024" in query
    assert "DN = 32024R1689" in query
    assert "AU = EN" in query
    assert "TI ~ 'children''s rights'" in query


def test_eurlex_artifact_record_uses_celex_key_and_pdf():
    record = build_artifact_record(
        "eurlex",
        {
            "celex": "32024R1689",
            "title": "Artificial Intelligence Act",
            "date": "2024-06-13",
            "pdf": "https://example.test/ai-act.pdf",
            "html": "https://example.test/ai-act.html",
        },
        1,
        Path("/tmp/scraper-run"),
    )

    assert record["source_id"] == "32024R1689"
    assert record["bibtex_key"] == "eurlex_32024R1689"
    assert record["year"] == 2024
    assert record["download_url"] == "https://example.test/ai-act.pdf"
    assert record["file_name"] == "eurlex_32024R1689.pdf"
    assert record["metadata_completeness"] == "minimal"


def test_expert_groups_artifact_record_keeps_minimal_metadata():
    record = build_artifact_record(
        "expert_groups",
        {
            "expert_group_id": "E03745",
            "group_title": "Example Group",
            "date": "17/05/2024",
            "title": "Meeting",
            "meeting_url": "https://ec.europa.eu/meeting",
            "doc_url": "https://ec.europa.eu/document/agenda.pdf",
            "doc_title": "Agenda",
        },
        1,
        Path("/tmp/scraper-run"),
    )

    assert record["bibtex_key"].startswith("expertgroups_E03745_17_05_2024_")
    assert record["title"] == "Agenda"
    assert record["year"] == 2024
    assert record["file_name"].endswith(".pdf")
    assert record["metadata_completeness"] == "minimal"


def test_safe_filename_has_fallback():
    assert safe_filename(" /bad:name* ") == "bad_name"
    assert safe_filename("???", fallback="fallback") == "fallback"


def test_download_validation_rejects_empty_artifacts():
    with pytest.raises(RuntimeError, match="empty"):
        validate_download_content(b"", Path("artifact.html"))


def test_download_validation_rejects_non_pdf_bytes_for_pdf():
    with pytest.raises(RuntimeError, match="valid PDF"):
        validate_download_content(b"<html>not a pdf</html>", Path("artifact.pdf"))


def test_download_validation_accepts_pdf_signature():
    validate_download_content(b"%PDF-1.7\nbody", Path("artifact.pdf"))


def test_eurlex_response_validation_surfaces_soap_fault():
    content = b"""<?xml version='1.0' encoding='UTF-8'?>
    <env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
      <env:Body>
        <env:Fault>
          <env:Reason>
            <env:Text xml:lang="en-US">Failed to assert identity with UsernameToken.</env:Text>
          </env:Reason>
        </env:Fault>
      </env:Body>
    </env:Envelope>"""

    with pytest.raises(RuntimeError, match="Failed to assert identity"):
        raise_for_eurlex_response(content, 500)


def test_eurlex_response_validation_rejects_http_errors_without_fault():
    with pytest.raises(RuntimeError, match="HTTP 503"):
        raise_for_eurlex_response(b"service unavailable", 503)


# --- a completed run is not scraped again (PY-58, spec D16) ---------------------------

import json
import sqlite3

from dl_lit import scraper_lab


def _lab_db(tmp_path, status, total=0, downloaded=0, failed=0, artifacts=0):
    db_path = tmp_path / "lab.db"
    conn = sqlite3.connect(db_path)
    scraper_lab.ensure_scraper_schema(conn)
    run_id = conn.execute(
        """
        INSERT INTO scraper_runs (source_type, query_json, status, total_count, downloaded_count, failed_count,
                                  started_at, finished_at)
        VALUES ('eurlex', ?, ?, ?, ?, ?, '2000-01-01 00:00:00', '2000-01-01 00:01:00')
        """,
        (json.dumps({"fm_coded": "REG"}), status, total, downloaded, failed),
    ).lastrowid
    for index in range(artifacts):
        conn.execute(
            "INSERT INTO scraper_artifacts (run_id, source_type, title, status) VALUES (?, 'eurlex', ?, 'downloaded')",
            (run_id, f"A{index}"),
        )
    conn.commit()
    conn.close()
    return str(db_path), run_id


def _run_row(db_path, run_id):
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT * FROM scraper_runs WHERE id = ?", (run_id,)).fetchone()
    artifacts = conn.execute("SELECT COUNT(*) FROM scraper_artifacts WHERE run_id = ?", (run_id,)).fetchone()[0]
    conn.close()
    return row, artifacts


def _scraped_again(_query):
    raise AssertionError("scraped again")


def test_a_completed_run_is_not_scraped_again(tmp_path, monkeypatch):
    db_path, run_id = _lab_db(tmp_path, "completed", total=3, downloaded=2, failed=1, artifacts=1)
    before = _run_row(db_path, run_id)
    monkeypatch.setattr(scraper_lab, "scrape_eurlex", _scraped_again)

    result = scraper_lab.execute_scraper_job(db_path, run_id, str(tmp_path / "artifacts"))

    assert result == {"run_id": run_id, "source_type": "eurlex", "total": 3, "downloaded": 2, "failed": 1, "replayed": True}
    assert _run_row(db_path, run_id) == before
    assert before[1] == 1


def test_a_pending_run_still_scrapes(tmp_path, monkeypatch):
    db_path, run_id = _lab_db(tmp_path, "pending")
    monkeypatch.setattr(scraper_lab, "scrape_eurlex", lambda _query: [])

    result = scraper_lab.execute_scraper_job(db_path, run_id, str(tmp_path / "artifacts"))

    assert result == {"run_id": run_id, "source_type": "eurlex", "total": 0, "downloaded": 0, "failed": 0}
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT status FROM scraper_runs WHERE id = ?", (run_id,)).fetchone()[0] == "completed"
    conn.close()


def test_every_connection_is_closed(tmp_path, monkeypatch):
    db_path, run_id = _lab_db(tmp_path, "completed", total=3, downloaded=2, failed=1)
    monkeypatch.setattr(scraper_lab, "scrape_eurlex", _scraped_again)
    opened = []
    real_connect = sqlite3.connect

    def recording_connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr(scraper_lab.sqlite3, "connect", recording_connect)

    scraper_lab.execute_scraper_job(db_path, run_id, str(tmp_path / "artifacts"))
    with pytest.raises(ValueError, match="Unknown scraper run id"):
        scraper_lab.execute_scraper_job(db_path, run_id + 100, str(tmp_path / "artifacts"))

    assert len(opened) == 2
    for conn in opened:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
