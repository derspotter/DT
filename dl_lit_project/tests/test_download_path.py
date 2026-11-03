"""One download path: download_ref, the raw-download payload, the CLI counter."""
import builtins
import hashlib
from datetime import datetime as _datetime

from click.testing import CliRunner

import dl_lit.db_manager as db_manager_module
import dl_lit.download_path as download_path
from dl_lit.cli import cli
from dl_lit.db_manager import DatabaseManager
from dl_lit.download_path import download_ref


class _Enhancer:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def enhance_bibliography(self, ref, download_dir):
        self.calls.append((ref, download_dir))
        return self.result


# --- (a) download_ref -------------------------------------------------------

def test_download_ref_uses_the_downloaders_checksum_without_reading_the_file(tmp_path, monkeypatch):
    pdf = tmp_path / "paper.pdf"
    pdf.write_bytes(b"%PDF-1.4 content")
    trace = [{"source": "OpenAlex", "outcome": "success"}]
    enhancer = _Enhancer({"downloaded_file": str(pdf), "checksum": "abc", "download_trace": trace})

    def _boom(*args, **kwargs):
        raise AssertionError("the file must not be read when the downloader supplied a checksum")

    monkeypatch.setattr(builtins, "open", _boom)
    monkeypatch.setattr(hashlib, "file_digest", _boom)

    result, checksum, got_trace = download_ref(enhancer, {"title": "T"}, tmp_path)

    assert checksum == "abc"
    assert result["downloaded_file"] == str(pdf)
    assert got_trace == trace
    assert got_trace is not trace  # a copy, callers may mutate it


def test_download_ref_hashes_the_file_in_chunks_when_no_checksum_is_given(tmp_path, monkeypatch):
    pdf = tmp_path / "paper.pdf"
    data = b"%PDF-1.4 " + b"x" * 300_000
    pdf.write_bytes(data)
    enhancer = _Enhancer({"downloaded_file": str(pdf)})
    digests = []
    real_file_digest = hashlib.file_digest

    def _spy(fh, digest):
        digests.append(digest)
        return real_file_digest(fh, digest)

    monkeypatch.setattr(download_path.hashlib, "file_digest", _spy)

    result, checksum, trace = download_ref(enhancer, {"title": "T"}, tmp_path)

    assert checksum == hashlib.sha256(data).hexdigest()
    assert digests == ["sha256"]
    assert result["downloaded_file"] == str(pdf)
    assert trace == []


def test_download_ref_failure_returns_none_and_the_trace_of_the_ref(tmp_path):
    ref = {"title": "T", "download_trace": [{"source": "DOI", "outcome": "failure"}]}

    result, checksum, trace = download_ref(_Enhancer(None), ref, tmp_path)

    assert (result, checksum) == (None, "")
    assert trace == [{"source": "DOI", "outcome": "failure"}]


def test_download_ref_unreadable_file_gives_an_empty_checksum(tmp_path):
    enhancer = _Enhancer({"downloaded_file": str(tmp_path / "missing.pdf")})

    result, checksum, _ = download_ref(enhancer, {"title": "T"}, tmp_path)

    assert result is not None and checksum == ""


# --- (b) mark_raw_work_downloaded payloads ----------------------------------
# Snapshotted at a0e411d (before _raw_download_payload) with the same inputs and
# a frozen clock; the refactor must reproduce them exactly.

_DOWNLOAD_RESULT = {
    "title": "Canon", "authors": ["A. Author", "B. Author"], "type": "book", "year": 2020,
    "doi": None, "url": "https://example.org/x.pdf", "abstract": "Abs", "keywords": ["k1", "k2"],
    "source": "Src", "volume": "3", "issue": "4", "pages": "1--9", "publisher": "Pub",
    "downloaded_file": "/tmp/x.pdf", "download_source": "test", "checksum": "abc",
}

_SNAPSHOT_NON_DUPLICATE = {
    'id': 1, 'title': 'Canon', 'authors': '["A. Author", "B. Author"]', 'editors': None, 'year': 2020,
    'doi': None, 'normalized_doi': None, 'openalex_id': None, 'semantic_scholar_id': None,
    'source': 'Src', 'publisher': 'Pub', 'volume': '3', 'issue': '4', 'pages': '1--9', 'type': 'book',
    'url': 'https://example.org/x.pdf', 'open_access_url': None, 'isbn': None, 'issn': None,
    'abstract': 'Abs', 'keywords': '["k1", "k2"]', 'normalized_title': 'canon',
    'normalized_authors': 'a author b author', 'metadata_status': 'failed',
    'metadata_source': 'raw_download_fallback',
    'metadata_error': 'Metadata fetch failed; raw download fallback succeeded',
    'metadata_updated_at': '2026-01-02T03:04:05', 'download_status': 'downloaded',
    'download_source': 'test', 'download_error': None, 'downloaded_at': '2026-01-02T03:04:05',
    'metadata_claimed_by': None, 'metadata_claimed_at': None, 'metadata_lease_expires_at': None,
    'metadata_attempt_count': 0, 'metadata_last_attempt_at': None, 'download_claimed_by': None,
    'download_claimed_at': None, 'download_lease_expires_at': None, 'download_attempt_count': 0,
    'download_last_attempt_at': None, 'file_path': '/tmp/x.pdf', 'file_checksum': 'abc',
    'origin_type': None, 'origin_key': None, 'source_pdf': None, 'run_id': None,
    'source_work_id': None, 'relationship_type': None, 'referenced_work_ids': None,
    'cited_by_api_url': None, 'crossref_json': None, 'openalex_json': None,
    'bibtex_entry_json': (
        '{"title": "Canon", "authors": ["A. Author", "B. Author"], "type": "book", "year": 2020, '
        '"doi": null, "url": "https://example.org/x.pdf", "abstract": "Abs", "keywords": ["k1", "k2"], '
        '"source": "Src", "volume": "3", "issue": "4", "pages": "1--9", "publisher": "Pub", '
        '"downloaded_file": "/tmp/x.pdf", "download_source": "test", "checksum": "abc"}'
    ),
    'status_notes': 'Downloaded after metadata enrichment failed',
}
# The duplicate branch never set 'type': the source row's own type passes through.
_SNAPSHOT_DUPLICATE = {**_SNAPSHOT_NON_DUPLICATE, 'id': 2, 'type': 'article'}


class _FrozenDatetime(_datetime):
    @classmethod
    def now(cls, tz=None):
        return _datetime(2026, 1, 2, 3, 4, 5)


def _insert_work(db, title, **cols):
    data = {"title": title, "normalized_title": db._normalize_text(title), **cols}
    if data.get("doi"):
        data["normalized_doi"] = db._normalize_doi(data["doi"])
    names = ", ".join(data)
    marks = ", ".join("?" for _ in data)
    work_id = db.conn.execute(f"INSERT INTO works ({names}) VALUES ({marks})", tuple(data.values())).lastrowid
    db.conn.commit()
    return work_id


def _captured_payload(tmp_path, monkeypatch, duplicate):
    monkeypatch.setattr(db_manager_module, "datetime", _FrozenDatetime)
    db = DatabaseManager(str(tmp_path / ("dup.db" if duplicate else "solo.db")))
    if duplicate:
        canonical = _insert_work(db, "Canon", year=2020, doi="10.1/c")
    source = _insert_work(db, "Canon" if duplicate else "Solo", year=2020, type="article")
    seen = {}
    real_apply = db._apply_work_merge_priority
    real_update = db._update_work_record

    def _apply(existing, payload):
        seen["payload"] = dict(payload)
        return real_apply(existing, payload)

    def _update(work_id, payload):
        seen.setdefault("payload", dict(payload))
        return real_update(work_id, payload)

    monkeypatch.setattr(db, "_apply_work_merge_priority", _apply)
    monkeypatch.setattr(db, "_update_work_record", _update)
    result = db.mark_raw_work_downloaded(source, dict(_DOWNLOAD_RESULT), "/tmp/x.pdf", "abc", "test")
    expected_id = canonical if duplicate else source
    assert result == (expected_id, None)
    payload = seen["payload"]
    payload.pop("created_at")
    payload.pop("updated_at")
    return payload


def test_raw_download_payload_non_duplicate_branch_matches_the_snapshot(tmp_path, monkeypatch):
    assert _captured_payload(tmp_path, monkeypatch, duplicate=False) == _SNAPSHOT_NON_DUPLICATE


def test_raw_download_payload_duplicate_branch_matches_the_snapshot(tmp_path, monkeypatch):
    assert _captured_payload(tmp_path, monkeypatch, duplicate=True) == _SNAPSHOT_DUPLICATE


def test_raw_download_payload_branches_differ_only_in_type(tmp_path, monkeypatch):
    solo = _captured_payload(tmp_path, monkeypatch, duplicate=False)
    dup = _captured_payload(tmp_path, monkeypatch, duplicate=True)
    differing = {key for key in solo.keys() | dup.keys() if solo.get(key) != dup.get(key)}
    assert differing == {"id", "type"}


# --- (c) CLI counter ----------------------------------------------------------

def test_cli_downloaded_count_ignores_the_duplicate_merge_result(mocker, tmp_path):
    db = mocker.patch("dl_lit.cli.DatabaseManager", autospec=True).return_value
    enhancer = mocker.patch("dl_lit.cli.BibliographyEnhancer", autospec=True).return_value
    pdf = tmp_path / "paper.pdf"
    pdf.write_bytes(b"%PDF")
    db.fetch_download_queue.return_value = [
        {"id": 1, "title": "Already downloaded"},
        {"id": 2, "title": "Fresh paper"},
    ]
    db.check_if_exists.side_effect = [("works", 9, "normalized_title"), (None, None, None)]
    db.merge_duplicate_queued_work.return_value = (True, "merged into 9")
    enhancer.enhance_bibliography.return_value = {
        "downloaded_file": str(pdf), "download_source": "mock", "checksum": "abc",
    }

    result = CliRunner().invoke(cli, ["download-pdfs", "--db-path", str(tmp_path / "t.db"), "--download-dir", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "Worker finished: 1 downloaded, 0 failed." in result.output
    db.move_entry_to_downloaded.assert_called_once_with(2, enhancer.enhance_bibliography.return_value, str(pdf), "abc", "mock")


# --- the daemon callers ---------------------------------------------------------

def _worker_daemon(monkeypatch, tmp_path, enhancer):
    import threading

    from tests._worker_import import load_worker

    mod = load_worker("dt_worker_download_path", monkeypatch)
    assert mod.download_ref is download_ref
    daemon = object.__new__(mod.PipelineDaemon)
    daemon.db = DatabaseManager(str(tmp_path / "worker.db"))
    daemon.db_lock = threading.RLock()
    daemon.enhancer = enhancer
    daemon.download_dir = tmp_path
    return daemon


def test_worker_download_single_marks_downloaded_with_the_downloader_checksum(tmp_path, monkeypatch):
    pdf = tmp_path / "paper.pdf"
    pdf.write_bytes(b"%PDF")
    enhancer = _Enhancer({"downloaded_file": str(pdf), "checksum": "abc", "download_source": "mock",
                          "download_trace": [{"source": "mock"}]})
    daemon = _worker_daemon(monkeypatch, tmp_path, enhancer)
    qid = _insert_work(daemon.db, "Queued", year=2020, authors="not json", download_status="queued")

    status, info = daemon._download_single(daemon.db.get_entry_by_id("works", qid))

    assert status == "downloaded" and info["downloaded_id"] == qid and info["_trace"] == [{"source": "mock"}]
    assert enhancer.calls[0][0]["authors"] == ["not json"]
    row = daemon.db.get_entry_by_id("works", qid)
    assert (row["download_status"], row["file_checksum"]) == ("downloaded", "abc")


def test_worker_raw_download_failure_reports_the_refs_failure_fields(tmp_path, monkeypatch):
    class _Failing:
        def enhance_bibliography(self, ref, download_dir):
            ref["download_failure_category"] = "source_exhausted"
            ref["download_failure_detail"] = "nothing found"
            return None

    daemon = _worker_daemon(monkeypatch, tmp_path, _Failing())
    wid = _insert_work(daemon.db, "Raw", year=2020)

    ok, info = daemon._attempt_raw_download(daemon.db.get_entry_by_id("works", wid))

    assert not ok
    assert (info["failure_category"], info["failure_detail"]) == ("source_exhausted", "nothing found")
