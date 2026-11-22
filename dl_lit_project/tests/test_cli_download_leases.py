"""The CLI claims rows like the daemon, and no stale owner writes a result (PY-16, spec D2).

Every test runs on a temp DB. "Second connection" is a second DatabaseManager or sqlite3
connection on the same file. Lease expiry is driven by a fake clock (``time.time`` + offset),
never by sleeping.
"""
import hashlib
import os
import re
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

import dl_lit.db_manager as db_manager_module
import dl_lit.download_path as download_path
from dl_lit.cli import cli
from dl_lit.db_manager import DatabaseManager
from tests.test_download_path import _insert_work, _worker_daemon

CLAIM_LOST = "claim_lost"
LOST_LINE = "claim lost before its result was written; result discarded."
DAEMON = "daemon:h:1:abc"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _row(db_path, work_id):
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    try:
        row = c.execute("SELECT * FROM works WHERE id = ?", (work_id,)).fetchone()
        return dict(row) if row else None
    finally:
        c.close()


def _setup(tmp_path, n=1, *, name="lit.db", **cols):
    db_path = tmp_path / name
    db = DatabaseManager(str(db_path))
    cols.setdefault("download_status", "queued")
    ids = [_insert_work(db, f"Paper {i}", year=2020, **cols) for i in range(n)]
    db.close_connection()
    return db_path, ids


def _run_cli(db_path, download_dir, *extra):
    return CliRunner().invoke(
        cli, ["download-pdfs", "--db-path", str(db_path), "--download-dir", str(download_dir), *extra]
    )


def _fake_clock(monkeypatch):
    offset = [0]
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + offset[0])
    return offset


def _claim(db, work_id, claimed_by, phase="download"):
    if phase == "download":
        rows = db.claim_download_batch(limit=50, corpus_id=None, claimed_by=claimed_by, lease_seconds=900)
    else:
        rows = db.claim_enrich_batch(limit=50, corpus_id=None, claimed_by=claimed_by, lease_seconds=900)
    picked = [r for r in rows if int(r["id"]) == int(work_id)]
    assert picked, f"work {work_id} was not claimed"
    return picked[0]


class _StagingEnhancer:
    """Writes its bytes to ``<name><part_suffix>`` like the real builders and reports that path."""

    def __init__(self, data: bytes, name="2020_T.pdf", *, checksum=None, before=None):
        self.data, self.name, self.checksum, self.before = data, name, checksum, before
        self.calls = []

    def enhance_bibliography(self, ref, download_dir):
        self.calls.append(dict(ref))
        if self.before:
            self.before(ref)
        path = Path(download_dir) / (self.name + ref.get("part_suffix", ""))
        path.write_bytes(self.data)
        return {
            "downloaded_file": str(path),
            "download_source": "mock",
            "checksum": _sha(self.data) if self.checksum is None else self.checksum,
        }


def _parts(directory):
    return sorted(p.name for p in Path(directory).iterdir() if ".part-" in p.name)


# --- the CLI claims --------------------------------------------------------------------------

def test_cli_claims_a_row_before_downloading_it(tmp_path, mocker):
    db_path, (wid,) = _setup(tmp_path)
    mocker.patch("dl_lit.cli.BibliographyEnhancer")
    seen = []

    def fake(enhancer, ref, download_dir, **kwargs):
        row = _row(db_path, wid)
        seen.append((row["download_status"], str(row["download_claimed_by"] or "").startswith("cli:")))
        return None, "", []

    mocker.patch("dl_lit.cli.download_ref", side_effect=fake)
    result = _run_cli(db_path, tmp_path)

    assert result.exit_code == 0, result.output
    assert seen == [("in_progress", True)]
    row = _row(db_path, wid)
    assert (row["download_status"], row["download_claimed_by"]) == ("failed", None)


def test_the_daemon_cannot_claim_a_row_the_cli_is_downloading(tmp_path, mocker):
    db_path, _ = _setup(tmp_path)
    mocker.patch("dl_lit.cli.BibliographyEnhancer")
    recorded = []

    def fake(enhancer, ref, download_dir, **kwargs):
        other = DatabaseManager(str(db_path))
        try:
            recorded.append(other.claim_download_batch(limit=5, corpus_id=None, claimed_by=DAEMON, lease_seconds=900))
        finally:
            other.close_connection()
        return None, "", []

    mocker.patch("dl_lit.cli.download_ref", side_effect=fake)
    result = _run_cli(db_path, tmp_path)

    assert result.exit_code == 0, result.output
    assert recorded == [[]]


def test_the_cli_claims_one_row_at_a_time(tmp_path, mocker):
    db_path, _ = _setup(tmp_path, 3)
    mocker.patch("dl_lit.cli.BibliographyEnhancer")
    counts = []

    def fake(enhancer, ref, download_dir, **kwargs):
        c = sqlite3.connect(str(db_path))
        counts.append(c.execute(
            "SELECT COUNT(*) FROM works WHERE download_status = 'in_progress' AND download_claimed_by LIKE 'cli:%'"
        ).fetchone()[0])
        c.close()
        return None, "", []

    mocker.patch("dl_lit.cli.download_ref", side_effect=fake)
    result = _run_cli(db_path, tmp_path)

    assert result.exit_code == 0, result.output
    assert counts == [1, 1, 1]


@pytest.mark.parametrize("limit,calls,empty", [("-1", 3, False), ("0", 0, True)])
def test_cli_limit_keeps_its_sqlite_meaning(tmp_path, mocker, limit, calls, empty):
    db_path, ids = _setup(tmp_path, 3)
    mocker.patch("dl_lit.cli.BibliographyEnhancer")
    fake = mocker.patch("dl_lit.cli.download_ref", return_value=(None, "", []))

    result = _run_cli(db_path, tmp_path, "--limit", limit)

    assert result.exit_code == 0, result.output
    assert fake.call_count == calls
    assert ("Queue is empty." in result.output) is empty
    statuses = [_row(db_path, wid)["download_status"] for wid in ids]
    assert statuses == (["queued"] * 3 if empty else ["failed"] * 3)


def test_cli_takes_a_daemon_row_only_after_its_lease_expires(tmp_path, mocker, monkeypatch):
    offset = _fake_clock(monkeypatch)
    db_path, (wid,) = _setup(
        tmp_path, download_status="in_progress", download_claimed_by=DAEMON,
        download_lease_expires_at=int(time.time()) + 10,
    )
    mocker.patch("dl_lit.cli.BibliographyEnhancer")
    fake = mocker.patch("dl_lit.cli.download_ref", return_value=(None, "", []))

    first = _run_cli(db_path, tmp_path)
    assert first.exit_code == 0, first.output
    assert "Queue is empty." in first.output and fake.call_count == 0

    offset[0] = 11
    second = _run_cli(db_path, tmp_path)
    assert second.exit_code == 0, second.output
    assert fake.call_count == 1
    assert _row(db_path, wid)["download_status"] == "failed"


def test_a_stale_cli_completion_writes_nothing(tmp_path, mocker, monkeypatch):
    offset = _fake_clock(monkeypatch)
    db_path, (wid,) = _setup(tmp_path)
    mocker.patch("dl_lit.cli.BibliographyEnhancer")
    parts = []

    def fake(enhancer, ref, download_dir, part_suffix=None):
        offset[0] = 901
        other = DatabaseManager(str(db_path))
        try:
            assert other.claim_download_batch(limit=1, corpus_id=None, claimed_by=DAEMON, lease_seconds=900)
        finally:
            other.close_connection()
        part = Path(download_dir) / ("2020_T.pdf" + (part_suffix or ""))
        part.write_bytes(b"%PDF A")
        parts.append(part)
        return {"downloaded_file": str(part), "download_source": "mock", "checksum": _sha(b"%PDF A")}, _sha(b"%PDF A"), []

    mocker.patch("dl_lit.cli.download_ref", side_effect=fake)
    result = _run_cli(db_path, tmp_path)

    assert result.exit_code == 0, result.output
    row = _row(db_path, wid)
    assert (row["download_status"], row["download_claimed_by"]) == ("in_progress", DAEMON)
    assert f"Work {wid}: download {LOST_LINE}" in result.output
    assert parts and not parts[0].exists()
    assert not (tmp_path / "2020_T.pdf").exists()


def test_a_stale_daemon_completion_writes_nothing(tmp_path, monkeypatch):
    holder = {}

    def repoint(ref):
        c = sqlite3.connect(str(tmp_path / "worker.db"))
        c.execute("UPDATE works SET download_claimed_by = 'cli:h:1:abc' WHERE id = ?", (holder["wid"],))
        c.commit()
        c.close()

    daemon = _worker_daemon(monkeypatch, tmp_path, _StagingEnhancer(b"%PDF B", before=repoint))
    holder["wid"] = wid = _insert_work(daemon.db, "T", year=2020, download_status="queued")
    row = _claim(daemon.db, wid, daemon.worker_id)

    status, data = daemon._download_single(row)

    assert status == "skipped" and data["action"] == "claim_lost"
    got = _row(tmp_path / "worker.db", wid)
    assert (got["download_status"], got["download_claimed_by"]) == ("in_progress", "cli:h:1:abc")
    assert _parts(tmp_path) == [] and not (tmp_path / "2020_T.pdf").exists()


@pytest.mark.parametrize("other_status,merged", [("downloaded", True), ("not_requested", False)])
def test_cli_merges_only_into_a_downloaded_duplicate(tmp_path, mocker, other_status, merged):
    db_path = tmp_path / "lit.db"
    db = DatabaseManager(str(db_path))
    qid = _insert_work(db, "Same Title", year=2020, download_status="queued")
    other = _insert_work(db, "Same Title", year=2020, doi="10.1000/two", download_status=other_status)
    db.close_connection()
    mocker.patch("dl_lit.cli.BibliographyEnhancer")
    fake = mocker.patch("dl_lit.cli.download_ref", return_value=(None, "", []))

    result = _run_cli(db_path, tmp_path)

    assert result.exit_code == 0, result.output
    if merged:
        assert "duplicate detected" in result.output
        assert fake.call_count == 0
        assert _row(db_path, qid) is None and _row(db_path, other) is not None
    else:
        assert "duplicate detected" not in result.output
        assert fake.call_count == 1
        assert _row(db_path, qid)["download_status"] == "failed"


def test_cli_releases_its_claims_when_it_fails(tmp_path, mocker):
    db_path, ids = _setup(tmp_path, 2)
    mocker.patch("dl_lit.cli.BibliographyEnhancer")
    mocker.patch("dl_lit.cli.download_ref", side_effect=RuntimeError("boom"))

    result = _run_cli(db_path, tmp_path)

    assert result.exit_code != 0
    for wid in ids:
        row = _row(db_path, wid)
        assert (row["download_status"], row["download_claimed_by"], row["download_lease_expires_at"]) == ("queued", None, None)


# --- the daemon's download ---------------------------------------------------------------------

def test_daemon_download_uses_the_shared_duplicate_rule(tmp_path, monkeypatch):
    daemon = _worker_daemon(monkeypatch, tmp_path, _StagingEnhancer(b"%PDF"))
    wid = _insert_work(daemon.db, "T", year=2020, download_status="queued")
    row = _claim(daemon.db, wid, daemon.worker_id)
    calls = []

    def find(row_arg, authors):
        calls.append(int(row_arg["id"]))
        return "works", 9, "normalized_doi"

    monkeypatch.setattr(daemon.db, "find_downloaded_duplicate", find, raising=False)
    monkeypatch.setattr(daemon.db, "merge_duplicate_queued_work", lambda *a, **k: (True, "merged into 9"))

    assert daemon._download_single(row)[0] == "skipped"
    assert calls == [wid]


@pytest.mark.parametrize("lease_offset,released", [(900, False), (-1, True)])
def test_startup_recovery_keeps_a_live_cli_lease(tmp_path, monkeypatch, lease_offset, released):
    from tests.test_worker_recovery import _daemon

    db = DatabaseManager(str(tmp_path / "lit.db"))
    wid = _insert_work(
        db, "T", year=2020, download_status="in_progress", download_claimed_by="cli:otherhost:1:abc",
        download_claimed_at=int(time.time()), download_lease_expires_at=int(time.time()) + lease_offset,
    )
    db.close_connection()

    _daemon(monkeypatch, tmp_path, "download")

    row = _row(tmp_path / "lit.db", wid)
    got = (row["download_status"], row["download_claimed_by"], row["download_lease_expires_at"])
    if released:
        assert got == ("queued", None, None)
    else:
        assert got[:2] == ("in_progress", "cli:otherhost:1:abc") and got[2] is not None


def test_daemon_leases_outlast_its_watchdog(tmp_path, monkeypatch):
    from tests.test_worker_recovery import _daemon

    monkeypatch.delenv("RAG_FEEDER_PIPELINE_JOB_TIMEOUT", raising=False)
    monkeypatch.delenv("RAG_FEEDER_DIRECT_BATCH_TIMEOUT", raising=False)
    daemon = _daemon(monkeypatch, tmp_path, "all")
    assert daemon._claim_lease_seconds() == 1860

    monkeypatch.setenv("RAG_FEEDER_PIPELINE_JOB_TIMEOUT", "60")
    monkeypatch.setenv("RAG_FEEDER_DIRECT_BATCH_TIMEOUT", "60")
    short = _daemon(monkeypatch, tmp_path, "all")
    assert short._claim_lease_seconds() == 900

    for d in (daemon, short):
        leases = []
        monkeypatch.setattr(d.db, "claim_download_batch", lambda **kw: (leases.append(kw["lease_seconds"]), [])[1])
        monkeypatch.setattr(d.db, "claim_enrich_batch", lambda **kw: (leases.append(kw["lease_seconds"]), [])[1])
        d.do_download(None, 1)
        d.do_enrich(None, 1, 1, {})
        assert leases == [d._claim_lease_seconds()] * 2


# --- no download overwrites a published PDF ------------------------------------------------------

def test_a_late_attempt_leaves_the_winning_pdf_intact(tmp_path, mocker, monkeypatch):
    offset = _fake_clock(monkeypatch)
    daemon = _worker_daemon(monkeypatch, tmp_path, _StagingEnhancer(b"%PDF B"))
    db_path = tmp_path / "worker.db"
    wid = _insert_work(daemon.db, "T", year=2020, download_status="queued")

    def daemon_wins(ref):
        offset[0] = 1000
        row = _claim(daemon.db, wid, daemon.worker_id)
        status, _ = daemon._download_single(row)
        assert status == "downloaded"

    cli_enhancer = _StagingEnhancer(b"%PDF A")
    cli_enhancer.before = None
    real_enhance = cli_enhancer.enhance_bibliography

    def held(ref, download_dir):
        result = real_enhance(ref, download_dir)  # the CLI's bytes are staged ...
        daemon_wins(ref)                          # ... while the daemon takes over and finishes
        return result

    cli_enhancer.enhance_bibliography = held
    mocker.patch("dl_lit.cli.BibliographyEnhancer", return_value=cli_enhancer)

    result = _run_cli(db_path, tmp_path)

    assert result.exit_code == 0, result.output
    assert (tmp_path / "2020_T.pdf").read_bytes() == b"%PDF B"
    row = _row(db_path, wid)
    assert row["file_checksum"] == _sha(b"%PDF B")
    assert row["file_path"] == str(tmp_path / "2020_T.pdf")
    assert _parts(tmp_path) == []
    assert f"Work {wid}: download {LOST_LINE}" in result.output


def test_two_works_with_the_same_derived_name_keep_separate_files(tmp_path, monkeypatch):
    daemon = _worker_daemon(monkeypatch, tmp_path, None)
    first = _insert_work(daemon.db, "One", year=2020, download_status="queued")
    second = _insert_work(daemon.db, "Two", year=2021, download_status="queued")
    rows = {int(r["id"]): r for r in daemon.db.claim_download_batch(
        limit=2, corpus_id=None, claimed_by=daemon.worker_id, lease_seconds=900)}
    for wid, data in ((first, b"%PDF A"), (second, b"%PDF B")):
        daemon.enhancer = _StagingEnhancer(data, name="untitled.pdf")
        assert daemon._download_single(rows[wid])[0] == "downloaded"

    one, two = daemon.db.get_entry_by_id("works", first), daemon.db.get_entry_by_id("works", second)
    assert (one["file_path"], one["file_checksum"]) == (str(tmp_path / "untitled.pdf"), _sha(b"%PDF A"))
    assert (two["file_path"], two["file_checksum"]) == (str(tmp_path / f"untitled-{second}.pdf"), _sha(b"%PDF B"))
    assert Path(one["file_path"]).read_bytes() == b"%PDF A"
    assert Path(two["file_path"]).read_bytes() == b"%PDF B"

    part = tmp_path / "untitled.pdf.part-0123abcd"
    part.write_bytes(b"%PDF C")
    published = download_path.publish_download(str(part), second)
    assert re.fullmatch(rf"untitled-{second}-[0-9a-f]{{8}}\.pdf", Path(published).name)
    assert Path(published).read_bytes() == b"%PDF C"


def _published_first(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    (tmp_path / "untitled.pdf").write_bytes(b"%PDF A")
    first = _insert_work(db, "One", year=2020, download_status="downloaded",
                         file_path=str(tmp_path / "untitled.pdf"), file_checksum=_sha(b"%PDF A"))
    second = _insert_work(db, "Two", year=2021, download_status="queued")
    _claim(db, second, DAEMON)
    part = tmp_path / "untitled.pdf.part-11111111"
    part.write_bytes(b"%PDF B")
    return db, first, second, part


def test_a_failed_write_after_publishing_leaves_no_orphan(tmp_path, monkeypatch):
    db, first, second, part = _published_first(tmp_path)

    def boom(work_id, data):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(db, "_update_work_record", boom)
    wid, err = db.move_entry_to_downloaded(
        second, {"downloaded_file": str(part)}, str(part), _sha(b"%PDF B"), "mock",
        claimed_by=DAEMON, part_path=str(part),
    )

    assert wid is None and err
    assert not (tmp_path / f"untitled-{second}.pdf").exists()
    assert (tmp_path / "untitled.pdf").read_bytes() == b"%PDF A"
    row = _row(tmp_path / "lit.db", second)
    assert (row["download_status"], row["download_claimed_by"]) == ("in_progress", DAEMON)
    assert not db.conn.in_transaction


def test_a_failed_part_unlink_never_orphans_or_fails_the_publish(tmp_path, monkeypatch, capsys):
    real_unlink = os.unlink

    def unlink(path, *args, **kwargs):
        if ".part-" in os.path.basename(os.fspath(path)):
            raise OSError("read-only part")
        return real_unlink(path, *args, **kwargs)

    daemon = _worker_daemon(monkeypatch, tmp_path, _StagingEnhancer(b"%PDF A", name="x.pdf"))
    wid = _insert_work(daemon.db, "T", year=2020, download_status="queued")
    monkeypatch.setattr(os, "unlink", unlink)

    assert daemon._download_single(_claim(daemon.db, wid, daemon.worker_id))[0] == "downloaded"

    row = daemon.db.get_entry_by_id("works", wid)
    assert row["download_status"] == "downloaded"
    assert row["file_path"] == str(tmp_path / "x.pdf")
    assert Path(row["file_path"]).read_bytes() == b"%PDF A" and row["file_checksum"] == _sha(b"%PDF A")
    assert "read-only part" in capsys.readouterr().err

    # Same unlink failure with a DB failure after publishing: the writer still removes its PDF.
    daemon.enhancer = _StagingEnhancer(b"%PDF B", name="y.pdf")
    other = _insert_work(daemon.db, "U", year=2021, download_status="queued")
    row = _claim(daemon.db, other, daemon.worker_id)

    def boom(work_id, data):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(daemon.db, "_update_work_record", boom)
    assert daemon._download_single(row)[0] == "failed"
    assert not (tmp_path / "y.pdf").exists()


def test_an_unreadable_staged_pdf_is_not_published(tmp_path, monkeypatch):
    daemon = _worker_daemon(monkeypatch, tmp_path, _StagingEnhancer(b"%PDF A", name="x.pdf", checksum=""))
    wid = _insert_work(daemon.db, "T", year=2020, download_status="queued")

    def unreadable(fh, digest):
        raise OSError("unreadable")

    monkeypatch.setattr(download_path.hashlib, "file_digest", unreadable)
    status, data = daemon._download_single(_claim(daemon.db, wid, daemon.worker_id))

    assert status == "failed" and data["failure_category"] == "checksum_unavailable"
    assert not (tmp_path / "x.pdf").exists()
    assert _parts(tmp_path) == []
    row = daemon.db.get_entry_by_id("works", wid)
    assert (row["download_status"], row["download_error"]) == ("failed", "checksum_unavailable")


class _Response:
    status_code = 200

    def __init__(self, content):
        self.content = content
        self.headers = {"Content-Type": "application/pdf"}

    def raise_for_status(self):
        return None


class _Session:
    def __init__(self, content):
        self.content = content

    def get(self, *args, **kwargs):
        return _Response(self.content)


def _pdf_downloader(tmp_path, monkeypatch, content=b"%PDF-1.4 new"):
    import dl_lit.pdf_downloader as pdf_downloader

    class _NoLimit:
        def wait_if_needed(self, *args, **kwargs):
            return None

    monkeypatch.setattr(pdf_downloader, "get_thread_session", lambda name: _Session(content))
    monkeypatch.setattr(pdf_downloader.PDFDownloader, "validate_pdf", lambda self, content, ref_type=None: True)
    return pdf_downloader.PDFDownloader(tmp_path, rate_limiter=_NoLimit())


URL_ONLY = {"open_access": {"oa_url": "https://example.org/x.pdf"}}


def test_staged_files_are_created_exclusively(tmp_path, monkeypatch):
    downloader = _pdf_downloader(tmp_path, monkeypatch)
    published = tmp_path / "untitled.pdf"
    published.write_bytes(b"%PDF-1.4 published")
    left_behind = tmp_path / "untitled.pdf.part-x"
    os.link(published, left_behind)

    path, checksum, source, _ = downloader.attempt_download(URL_ONLY, part_suffix=".part-x")

    assert path is not None and Path(path) != left_behind.resolve()
    assert re.fullmatch(r"untitled\.pdf\.part-[0-9a-f]{8}", Path(path).name)
    assert Path(path).read_bytes() == b"%PDF-1.4 new"
    assert published.read_bytes() == b"%PDF-1.4 published"
    assert left_behind.read_bytes() == b"%PDF-1.4 published"
    Path(path).unlink()

    # A mid-write error: the builder removes the staged file it created.
    real_open = open

    class _Broken:
        def __init__(self, fh):
            self.fh = fh

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.fh.close()
            return False

        def write(self, data):
            raise OSError("disk full")

        def close(self):
            self.fh.close()

    def broken_open(file, mode="r", *args, **kwargs):
        fh = real_open(file, mode, *args, **kwargs)
        return _Broken(fh) if "x" in mode else fh

    monkeypatch.setattr(download_path, "open", broken_open, raising=False)
    with pytest.raises(OSError):
        downloader.attempt_download(URL_ONLY, part_suffix=".part-y")
    assert _parts(tmp_path) == ["untitled.pdf.part-x"]


def test_stored_metadata_names_the_retained_pdf(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    wid = _insert_work(db, "Solo", year=2020, download_status="queued")
    _claim(db, wid, DAEMON)
    part = tmp_path / "solo.pdf.part-22222222"
    part.write_bytes(b"%PDF A")
    result = {"downloaded_file": str(part), "part_suffix": ".part-22222222", "download_source": "mock",
              "download_trace": [{"detail": str(part)}]}
    got, err = db.move_entry_to_downloaded(wid, result, str(part), _sha(b"%PDF A"), "mock",
                                           claimed_by=DAEMON, part_path=str(part))
    assert (got, err) == (wid, None)
    row = db.get_entry_by_id("works", wid)
    stored = __import__("json").loads(row["bibtex_entry_json"])
    assert stored["downloaded_file"] == row["file_path"] == str(tmp_path / "solo.pdf")
    assert "part_suffix" not in stored

    # A raw merge into a canonical that keeps its own PDF.
    (tmp_path / "canon.pdf").write_bytes(b"%PDF canon")
    canonical = _insert_work(db, "Canon", year=2020, doi="10.1000/canon", download_status="downloaded",
                             file_path=str(tmp_path / "canon.pdf"), file_checksum=_sha(b"%PDF canon"))
    raw = _insert_work(db, "Canon", year=2020, metadata_status="pending", download_status="not_requested")
    _claim(db, raw, DAEMON, phase="enrich")
    part = tmp_path / "raw.pdf.part-33333333"
    part.write_bytes(b"%PDF raw")
    raw_result = {"title": "Canon", "year": 2020, "downloaded_file": str(part), "part_suffix": ".part-33333333",
                  "checksum": _sha(b"%PDF raw"), "download_source": "mock"}
    got, err = db.mark_raw_work_downloaded(raw, raw_result, str(part), _sha(b"%PDF raw"), "mock",
                                           claimed_by=DAEMON, part_path=str(part))
    assert (got, err) == (canonical, None)
    row = db.get_entry_by_id("works", canonical)
    assert row["file_path"] == str(tmp_path / "canon.pdf")
    stored = __import__("json").loads(row["bibtex_entry_json"])
    assert stored["downloaded_file"] == row["file_path"]
    assert stored.get("checksum", row["file_checksum"]) == row["file_checksum"] == _sha(b"%PDF canon")
    assert "part_suffix" not in stored
    assert not (tmp_path / "raw.pdf").exists()


def test_long_names_are_shortened_only_when_the_suffixes_do_not_fit():
    short = "a" * 200 + ".pdf"
    assert download_path.fit_stem(short) == short
    long_name = "ä" * 120 + ".pdf"
    assert len(os.fsencode(long_name)) == 244
    fitted = download_path.fit_stem(long_name)
    assert fitted != long_name and fitted.endswith(".pdf")
    assert len(os.fsencode(fitted)) + 32 <= 255
    assert os.fsencode(fitted).decode("utf-8") == fitted
    assert long_name.startswith(fitted[:-4])


def test_owned_writers_end_their_transaction_on_every_exit(tmp_path, monkeypatch):
    # (a) the merge's target is gone: the fallback download runs with no write lock held.
    db_path = tmp_path / "worker.db"
    free = []

    def probe(ref):
        c = sqlite3.connect(str(db_path), timeout=0, isolation_level=None)
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("ROLLBACK")
            free.append(True)
        finally:
            c.close()

    daemon = _worker_daemon(monkeypatch, tmp_path, _StagingEnhancer(b"%PDF A", name="a.pdf", before=probe))
    wid = _insert_work(daemon.db, "T", year=2020, download_status="queued")
    target = _insert_work(daemon.db, "T", year=2020, doi="10.1000/t", download_status="not_requested")
    row = _claim(daemon.db, wid, daemon.worker_id)
    monkeypatch.setattr(daemon.db, "find_downloaded_duplicate", lambda r, a: ("works", target, "title"), raising=False)

    assert daemon._download_single(row)[0] == "downloaded"
    assert free == [True]

    # (b) os.link fails: an error is returned, the transaction is closed, the claim is kept.
    db, first, second, part = _published_first(tmp_path)

    def no_link(src, dst, *args, **kwargs):
        raise OSError("link refused")

    monkeypatch.setattr(os, "link", no_link)
    got, err = db.move_entry_to_downloaded(second, {}, str(part), _sha(b"%PDF B"), "mock",
                                           claimed_by=DAEMON, part_path=str(part))
    assert got is None and err.startswith("publish failed:")
    assert not db.conn.in_transaction
    row = _row(tmp_path / "lit.db", second)
    assert (row["download_status"], row["download_claimed_by"]) == ("in_progress", DAEMON)
    assert not (tmp_path / f"untitled-{second}.pdf").exists()


def test_the_writers_write_part_files(tmp_path, monkeypatch):
    import dl_lit.new_dl as new_dl

    downloader = _pdf_downloader(tmp_path, monkeypatch)
    path, _, _, _ = downloader.attempt_download(URL_ONLY, part_suffix=".part-x")
    assert Path(path).name == "untitled.pdf.part-x" and not (tmp_path / "untitled.pdf").exists()
    path, _, _, _ = downloader.attempt_download(URL_ONLY)
    assert Path(path).name == "untitled.pdf"
    (tmp_path / "untitled.pdf").unlink()

    enhancer = new_dl.BibliographyEnhancer(db_manager=None, rate_limiter=None, llm_optional=True)
    monkeypatch.setattr(enhancer, "_request_get", lambda url, **kw: (_Response(b"%PDF-1.4 direct"), "direct"))
    monkeypatch.setattr(enhancer, "_get_pdf_downloader", lambda d: downloader)
    ref = {"title": "Direct", "year": 2020, "part_suffix": ".part-y"}
    assert enhancer.try_download_url("https://example.org/d.pdf", "Direct URL", ref, tmp_path) is True
    assert Path(ref["downloaded_file"]).name == "2020_Direct.pdf.part-y"
    assert not (tmp_path / "2020_Direct.pdf").exists()
    plain = {"title": "Direct", "year": 2020}
    assert enhancer.try_download_url("https://example.org/d.pdf", "Direct URL", plain, tmp_path) is True
    assert Path(plain["downloaded_file"]).name == "2020_Direct.pdf"

    staged = tmp_path / "2020_Direct.pdf.part-y"
    (tmp_path / "2020_Direct.pdf").unlink()
    published = download_path.publish_download(str(staged), 7)
    assert published == str(tmp_path / "2020_Direct.pdf")
    assert Path(published).read_bytes() == b"%PDF-1.4 direct"
    assert staged.exists()  # left for the caller's discard_download
    download_path.discard_download(str(staged))
    assert not staged.exists()


def test_the_guarded_merge_rechecks_its_target(tmp_path, monkeypatch):
    daemon = _worker_daemon(monkeypatch, tmp_path, _StagingEnhancer(b"%PDF A", name="g.pdf"))
    wid = _insert_work(daemon.db, "Same Title", year=2020, download_status="queued")
    target = _insert_work(daemon.db, "Same Title", year=2020, doi="10.1000/t", download_status="downloaded")
    row = _claim(daemon.db, wid, daemon.worker_id)
    real_find = daemon.db.find_downloaded_duplicate
    real_merge = daemon.db.merge_duplicate_queued_work
    merges = []

    def find(row_arg, authors):
        hit = real_find(row_arg, authors)
        assert hit[1] == target
        c = sqlite3.connect(str(tmp_path / "worker.db"))
        c.execute("UPDATE works SET download_status = 'failed' WHERE id = ?", (target,))
        c.commit()
        c.close()
        return hit

    def merge(*args, **kwargs):
        merges.append(real_merge(*args, **kwargs))
        return merges[-1]

    monkeypatch.setattr(daemon.db, "find_downloaded_duplicate", find)
    monkeypatch.setattr(daemon.db, "merge_duplicate_queued_work", merge)

    assert daemon._download_single(row)[0] == "downloaded"
    assert merges == [(False, "duplicate_target_gone")]
    assert daemon.db.get_entry_by_id("works", wid)["download_status"] == "downloaded"


def test_a_writer_that_fails_part_way_rolls_back_all_of_it(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    canonical = _insert_work(db, "Canon", year=2020, doi="10.1000/canon", metadata_status="matched")
    row_id = _insert_work(db, "Canon", year=2020, metadata_status="pending")
    _claim(db, row_id, "daemon:t:1:a", phase="enrich")
    before = _row(tmp_path / "lit.db", canonical)
    real_update = db._update_work_record

    def update(work_id, data):
        if int(work_id) == canonical:
            raise sqlite3.OperationalError("disk I/O error")
        return real_update(work_id, data)

    monkeypatch.setattr(db, "_update_work_record", update)
    wid, err = db.apply_enriched_metadata(row_id, {"title": "Canon", "year": 2020}, claimed_by="daemon:t:1:a")

    assert wid is None and err
    row = _row(tmp_path / "lit.db", row_id)
    assert row is not None and (row["metadata_status"], row["metadata_claimed_by"]) == ("in_progress", "daemon:t:1:a")
    assert _row(tmp_path / "lit.db", canonical) == before
    assert not db.conn.in_transaction


WRITERS = ["apply_enriched_metadata", "mark_metadata_failed", "reset_enrich_claim", "mark_raw_work_downloaded",
           "move_entry_to_downloaded", "mark_download_failed", "merge_duplicate_queued_work"]


@pytest.mark.parametrize("writer", WRITERS)
def test_a_writer_whose_claim_was_lost_writes_nothing(tmp_path, writer):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    target = _insert_work(db, "Other", year=2001, doi="10.1000/other", download_status="downloaded")
    wid = _insert_work(db, "Mine", year=2020, metadata_status="in_progress", metadata_claimed_by="other",
                       download_status="in_progress", download_claimed_by="other")
    part = tmp_path / "mine.pdf.part-44444444"
    part.write_bytes(b"%PDF")
    result = {"downloaded_file": str(part), "download_source": "mock", "checksum": "abc"}
    before = _row(tmp_path / "lit.db", wid)
    calls = {
        "apply_enriched_metadata": lambda: db.apply_enriched_metadata(wid, {"title": "New"}, claimed_by="me"),
        "mark_metadata_failed": lambda: db.mark_metadata_failed(wid, "r", claimed_by="me"),
        "reset_enrich_claim": lambda: db.reset_enrich_claim(wid, state="pending", error=None, claimed_by="me"),
        "mark_raw_work_downloaded": lambda: db.mark_raw_work_downloaded(
            wid, result, str(part), "abc", "mock", claimed_by="me", part_path=str(part)),
        "move_entry_to_downloaded": lambda: db.move_entry_to_downloaded(
            wid, result, str(part), "abc", "mock", claimed_by="me", part_path=str(part)),
        "mark_download_failed": lambda: db.mark_download_failed(wid, "r", claimed_by="me"),
        "merge_duplicate_queued_work": lambda: db.merge_duplicate_queued_work(
            wid, "works", target, "title", claimed_by="me"),
    }

    got, err = calls[writer]()

    assert err == CLAIM_LOST == getattr(db_manager_module, "CLAIM_LOST", None)
    assert not got
    assert _row(tmp_path / "lit.db", wid) == before
    assert not (tmp_path / "mine.pdf").exists()
    assert not db.conn.in_transaction


# --- enrich deferral and recovery -------------------------------------------------------------

def test_a_deferred_enrich_releases_its_unstarted_rows_and_the_continuation_claims_them(tmp_path, monkeypatch):
    from tests.test_worker_recovery import _daemon

    monkeypatch.setenv("RAG_FEEDER_ENRICH_WAVE_SIZE", "2")
    daemon = _daemon(monkeypatch, tmp_path, "enrich")
    corpus = 5
    ids = [_insert_work(daemon.db, f"P{i}", year=2020, metadata_status="pending") for i in range(4)]
    for wid in ids:
        daemon.db.conn.execute("INSERT INTO corpus_works (corpus_id, work_id) VALUES (?, ?)", (corpus, wid))
    daemon.db.conn.commit()
    seen = []

    def enrich_single(entry, *args):
        seen.append(int(entry["id"]))
        return int(entry["id"]), None, {"status": "no_match"}

    monkeypatch.setattr(daemon, "_enrich_single", enrich_single)
    monkeypatch.setattr(daemon, "_attempt_raw_download", lambda entry: (False, {}))
    monkeypatch.setattr(daemon, "_has_pending_corpus_download_jobs", lambda: True)

    result = daemon.do_enrich(corpus, 4, 2, {})

    assert result["remaining_pending_work_ids"] == ids[2:]
    for wid in ids[2:]:
        row = daemon.db.get_entry_by_id("works", wid)
        assert (row["metadata_status"], row["metadata_claimed_by"], row["metadata_claimed_at"],
                row["metadata_lease_expires_at"], row["metadata_attempt_count"]) == ("pending", None, None, None, 0)

    seen.clear()
    daemon.do_enrich(corpus, 2, 2, {}, pending_work_ids=result["remaining_pending_work_ids"])
    assert sorted(seen) == ids[2:]


def _dead_pid():
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


@pytest.mark.parametrize("owner,released", [
    ("cli:{host}:{live_pid}:abc", False),
    ("cli:{host}:{dead_pid}:abc", True),
    ("cli:otherhost:1:abc", False),
])
def test_periodic_recovery_treats_a_cli_owner_like_a_daemon(tmp_path, monkeypatch, owner, released):
    from tests.test_worker_recovery import _daemon

    owner = owner.format(host=socket.gethostname(), live_pid=os.getpid(),
                         dead_pid=_dead_pid() if "dead_pid" in owner else 0)
    daemon = _daemon(monkeypatch, tmp_path, "download")
    wid = _insert_work(daemon.db, "T", year=2020, download_status="in_progress", download_claimed_by=owner,
                       download_claimed_at=int(time.time()), download_lease_expires_at=int(time.time()) + 900)

    daemon._recover_stale_running_jobs(periodic=True)

    row = _row(tmp_path / "lit.db", wid)
    if released:
        assert (row["download_status"], row["download_claimed_by"]) == ("queued", None)
    else:
        assert (row["download_status"], row["download_claimed_by"]) == ("in_progress", owner)
