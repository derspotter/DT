"""A hung item fails after 3 hangs (K-M4, spec D3).

The worker tracks which items are running; a watchdog win latches that set, fails the items that
ran past half the deadline once they reach the attempt limit, then reports within phase 5's exit
bound. A late result writes nothing. Clocks are fake (``daemon._running_clock``) except in the
exit-bound test; every exit goes through ``daemon._exit`` (``exits.append`` here).
"""
import contextlib
import sqlite3
import threading
import time

import pytest

from tests._worker_import import load_worker
from tests.test_download_path import _insert_work

DEADLINE = 0.3
LIMIT_ERROR = "hung past the 0.3s timeout; attempt limit 3 reached"


def _daemon(monkeypatch, tmp_path, role):
    mod = load_worker("dt_worker_hung_items", monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RAG_FEEDER_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.delenv("RAG_FEEDER_LLM_PROVIDER", raising=False)
    monkeypatch.delenv("RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS", raising=False)
    monkeypatch.setattr(mod.PipelineDaemon, "_build_enhancer", lambda self, *a: object())
    daemon = mod.PipelineDaemon(str(tmp_path / "lit.db"), role=role)
    daemon.download_dir = str(tmp_path)
    exits = []
    daemon._exit = exits.append
    clock = [0.0]
    daemon._running_clock = lambda: clock[0]
    lines = []
    real = mod.log
    monkeypatch.setattr(mod, "log", lambda msg: (lines.append(msg), real(msg)))
    return mod, daemon, exits, clock, lines


def _connect(tmp_path):
    return sqlite3.connect(tmp_path / "lit.db", timeout=5)


def _download_row(tmp_path, wid):
    c = _connect(tmp_path)
    row = c.execute(
        "SELECT download_status, download_error, download_claimed_by FROM works WHERE id = ?", (wid,)
    ).fetchone()
    c.close()
    return row


def _metadata_row(tmp_path, wid):
    c = _connect(tmp_path)
    row = c.execute(
        "SELECT metadata_status, metadata_error, metadata_claimed_by FROM works WHERE id = ?", (wid,)
    ).fetchone()
    c.close()
    return row


def _heartbeat_status(tmp_path, daemon):
    c = _connect(tmp_path)
    row = c.execute("SELECT status FROM daemon_heartbeat WHERE daemon_name = ?", (daemon.daemon_name,)).fetchone()
    c.close()
    return row[0]


def _claimed_download(daemon, title, attempts, owner=None):
    now = int(time.time())
    return _insert_work(
        daemon.db, title, year=2020, download_status="in_progress",
        download_claimed_by=owner or daemon.worker_id, download_claimed_at=now,
        download_lease_expires_at=now + 1800, download_attempt_count=attempts,
    )


class _ConnProxy:
    """Delegates to a real sqlite3 connection (whose attributes cannot be patched)."""

    def __init__(self, conn):
        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._conn, name)


# --- tracking ----------------------------------------------------------------------------------

def test_a_download_is_tracked_only_while_it_runs(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
    ids = [_insert_work(daemon.db, f"Q{i}", year=2020, download_status="queued") for i in range(2)]
    seen = {}

    def download_single(row):
        seen[int(row["id"])] = set(daemon._running_items)
        return "failed", {"queue_id": int(row["id"]), "action": "failed", "failure_category": "x"}

    monkeypatch.setattr(daemon, "_download_single", download_single)
    daemon.do_download(None, 5)

    assert sorted(seen) == sorted(ids)
    for wid, running in seen.items():
        assert ("download", wid) in running
    assert daemon._running_items == {}


def test_enrichment_and_its_raw_download_are_tracked(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "enrich")
    wid = _insert_work(daemon.db, "P", year=2020, metadata_status="pending")
    seen = {}

    def enrich_single(entry, *args):
        seen["enrich"] = set(daemon._running_items)
        return int(entry["id"]), None, {"status": "api_error"}

    def raw(enhancer, ref, download_dir, *, part_suffix=None):
        seen["raw"] = set(daemon._running_items)
        return None, "", []

    monkeypatch.setattr(daemon, "_enrich_single", enrich_single)
    monkeypatch.setattr(mod, "download_ref", raw)
    daemon.do_enrich(None, 5, 2, {})

    assert ("enrich", wid) in seen["enrich"]
    assert ("enrich", wid) in seen["raw"]
    assert daemon._running_items == {}


def test_no_item_starts_after_the_latch(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
    daemon._latch_running_items()
    with pytest.raises(mod.ItemsLatched):
        with daemon._item_running("download", 1):
            pass
    assert daemon._running_items == {}


def test_hung_item_limit_reads_env_with_minimum(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
    assert daemon._hung_item_limit() == 3
    monkeypatch.setenv("RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS", "0")
    assert daemon._hung_item_limit() == 1
    monkeypatch.setenv("RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS", "5")
    assert daemon._hung_item_limit() == 5


# --- the win -----------------------------------------------------------------------------------

def test_win_fails_only_overdue_running_items_at_the_limit(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
    a = _claimed_download(daemon, "A", 3)
    b = _claimed_download(daemon, "B", 2)
    c = _claimed_download(daemon, "C", 3)
    d = _claimed_download(daemon, "D", 3, owner="daemon:x:1:zzz")
    e = _claimed_download(daemon, "E", 3)
    f = _claimed_download(daemon, "F", 3)  # ran exactly half the deadline: counted
    g = _claimed_download(daemon, "G", 3)  # ran just under half: spared
    d_before = _download_row(tmp_path, d)
    gen = daemon._begin_work()
    with contextlib.ExitStack() as stack:
        for wid in (a, b, d):
            stack.enter_context(daemon._item_running("download", wid))
        for start, wid in ((DEADLINE * 0.5, f), (0.16, g), (0.29, e)):
            clock[0] = start
            stack.enter_context(daemon._item_running("download", wid))
        clock[0] = DEADLINE
        assert daemon._direct_batch_deadline_reached(gen, {"phase": "download"}, DEADLINE) is True

    assert exits == [75]
    assert _download_row(tmp_path, a) == ("failed", LIMIT_ERROR, None)
    assert _download_row(tmp_path, f) == ("failed", LIMIT_ERROR, None)
    for wid in (b, c, e, g):
        assert _download_row(tmp_path, wid)[0] == "in_progress"
        assert _download_row(tmp_path, wid)[2] == daemon.worker_id
    assert _download_row(tmp_path, d) == d_before
    assert "Watchdog: 2 hung item(s) failed at the attempt limit, 4 left for a retry after the restart." in lines


def test_a_late_result_after_the_win_writes_nothing(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
    a = _insert_work(daemon.db, "A", year=2020, download_status="queued", download_attempt_count=2)
    (row,) = daemon.db.claim_download_batch(limit=5, corpus_id=None, claimed_by=daemon.worker_id, lease_seconds=900)
    gen = daemon._begin_work()

    def download_ref(enhancer, ref, download_dir, *, part_suffix=None):
        clock[0] = DEADLINE
        daemon._direct_batch_deadline_reached(gen, {"phase": "download"}, DEADLINE)
        path = tmp_path / f"late.pdf{part_suffix}"
        path.write_bytes(b"%PDF")
        return {"downloaded_file": str(path), "download_source": "mock"}, "abc", []

    monkeypatch.setattr(mod, "download_ref", download_ref)
    with daemon._item_running("download", a):
        status, data = daemon._download_single(row)

    assert exits == [75]
    assert data["action"] == "claim_lost"
    assert _download_row(tmp_path, a) == ("failed", LIMIT_ERROR, None)
    assert not (tmp_path / "late.pdf").exists()


def test_an_item_that_hangs_every_time_fails_on_its_third_claim(tmp_path, monkeypatch):
    mod, first, *_ = _daemon(monkeypatch, tmp_path, "download")
    wid = _insert_work(first.db, "Hangs", year=2020, download_status="queued")
    for round_no in (1, 2, 3):
        mod, d, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
        if round_no > 1:
            assert _download_row(tmp_path, wid)[0] == "queued"
        rows = d.db.claim_download_batch(limit=5, corpus_id=None, claimed_by=d.worker_id, lease_seconds=900)
        assert [int(r["id"]) for r in rows] == [wid]
        with d._item_running("download", wid):
            clock[0] = DEADLINE
            d._direct_batch_deadline_reached(d._begin_work(), {"phase": "download"}, DEADLINE)
        assert exits == [75]
        expected = "failed" if round_no == 3 else "in_progress"
        assert _download_row(tmp_path, wid)[0] == expected
    assert _download_row(tmp_path, wid)[1] == LIMIT_ERROR
    mod, last, *_ = _daemon(monkeypatch, tmp_path, "download")
    assert last.db.claim_download_batch(limit=5, corpus_id=None, claimed_by=last.worker_id, lease_seconds=900) == []


def test_a_job_win_blames_running_items_too(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "enrich")
    c = _connect(tmp_path)
    job_id = c.execute(
        "INSERT INTO pipeline_jobs (job_type, status, worker_id, started_at) VALUES ('enrich', 'running', ?, datetime('now'))",
        (daemon.worker_id,),
    ).lastrowid
    c.commit()
    c.close()
    wid = _insert_work(daemon.db, "P", year=2020, metadata_status="pending", metadata_attempt_count=2)
    (row,) = daemon.db.claim_enrich_batch(limit=5, corpus_id=None, claimed_by=daemon.worker_id, lease_seconds=900)
    gen = daemon._begin_work()
    with daemon._item_running("enrich", wid):
        clock[0] = DEADLINE
        assert daemon._job_deadline_reached(gen, {"job_id": job_id, "job_type": "enrich"}, DEADLINE) is True

    assert exits == [75]
    status, error, owner = _metadata_row(tmp_path, wid)
    assert status == "failed" and error.startswith("hung past the 0.3s timeout") and owner is None


# --- the reporting budget ------------------------------------------------------------------------

def _busy_proxy(daemon, clock, advance, fail_commit):
    busy_values = []

    class Proxy(_ConnProxy):
        pending = False

        def execute(self, sql, *args):
            if sql.startswith("PRAGMA busy_timeout ="):
                busy_values.append(int(sql.split("=")[1]))
            result = self._conn.execute(sql, *args)
            if sql.lstrip().startswith("UPDATE works"):
                clock[0] += advance
                Proxy.pending = fail_commit
            return result

        def commit(self):
            if Proxy.pending:
                Proxy.pending = False
                raise sqlite3.OperationalError("database is locked")
            self._conn.commit()

    daemon.heartbeat_conn = Proxy(daemon.heartbeat_conn)
    return busy_values


def test_reporting_shares_one_busy_budget(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
    a = _claimed_download(daemon, "A", 3)
    busy_values = _busy_proxy(daemon, clock, 0.75, fail_commit=True)
    gen = daemon._begin_work()
    with daemon._item_running("download", a):
        clock[0] = 0.5  # 1.5 - 1.25 is exactly 0.25 in binary
        daemon._direct_batch_deadline_reached(gen, {"phase": "download"}, DEADLINE)

    assert busy_values[:2] == [1000, 250]
    assert _download_row(tmp_path, a)[0] == "in_progress"
    assert not daemon.heartbeat_conn.in_transaction
    assert "Watchdog: hung items not recorded (database is locked)." in lines
    assert _heartbeat_status(tmp_path, daemon) == "error"
    assert exits == [75]


def test_a_spent_budget_skips_the_heartbeat(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
    a = _claimed_download(daemon, "A", 3)
    prior = daemon.heartbeat_conn.execute("PRAGMA busy_timeout").fetchone()[0]
    busy_values = _busy_proxy(daemon, clock, 1.0, fail_commit=False)
    gen = daemon._begin_work()
    with daemon._item_running("download", a):
        clock[0] = DEADLINE
        daemon._direct_batch_deadline_reached(gen, {"phase": "download"}, DEADLINE)

    assert _download_row(tmp_path, a)[0] == "failed"
    assert busy_values == [1000, prior]  # the hung write, then the restore: none for the heartbeat
    assert _heartbeat_status(tmp_path, daemon) != "error"
    assert "Watchdog: terminal error heartbeat skipped (reporting budget spent)." in lines
    assert exits == [75]


def test_a_failed_rollback_never_reuses_the_connection(tmp_path, monkeypatch):
    """The hung UPDATE's commit fails and so does its rollback: the watchdog closes the
    connection and keeps heartbeat_lock through the exit, so no other thread's heartbeat (the
    main thread's next one after a processed batch) can commit the unrecorded write."""
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
    a = _claimed_download(daemon, "A", 3)
    real = daemon.heartbeat_conn
    commits = []

    class Broken(_ConnProxy):
        def commit(self):
            commits.append(1)
            if len(commits) == 1:  # the hung write's commit; a later one would persist it
                raise sqlite3.OperationalError("disk I/O error")
            self._conn.commit()

        def rollback(self):
            raise sqlite3.OperationalError("disk I/O error")

    daemon.heartbeat_conn = Broken(real)
    parked, release = threading.Event(), threading.Event()

    def park_in_exit(code):
        # The process would end here; the watchdog thread waits instead, still in its exit.
        exits.append(code)
        parked.set()
        release.wait(5)

    daemon._exit = park_in_exit
    gen = daemon._begin_work()
    watchdog = None
    try:
        with daemon._item_running("download", a):
            clock[0] = DEADLINE
            # The watchdog runs on its heartbeat thread, as in the daemon.
            watchdog = threading.Thread(
                target=daemon._direct_batch_deadline_reached, args=(gen, {"phase": "download"}, DEADLINE),
            )
            watchdog.start()
            assert parked.wait(5)
        assert exits == [75]

        acquired = daemon.heartbeat_lock.acquire(blocking=False)
        if acquired:
            daemon.heartbeat_lock.release()
        assert not acquired  # held through the exit: no other heartbeat can reach the connection

        assert commits == [1]
        with pytest.raises(sqlite3.ProgrammingError):
            real.execute("SELECT 1")  # closed: its open transaction is discarded
        other = _connect(tmp_path)
        try:
            other.execute("BEGIN IMMEDIATE")  # the write lock is free again
            assert other.execute("SELECT download_status FROM works WHERE id = ?", (a,)).fetchone()[0] == "in_progress"
            other.rollback()
        finally:
            other.close()
        assert "Watchdog: hung items not recorded (disk I/O error)." in lines
        assert any(line.startswith("Watchdog: terminal error heartbeat skipped (") for line in lines)
        assert _heartbeat_status(tmp_path, daemon) != "error"
    finally:
        release.set()
        if watchdog is not None:
            watchdog.join(5)
    assert not watchdog.is_alive()


def test_a_locked_database_keeps_the_phase5_exit_bound(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
    daemon._running_clock = time.monotonic
    a = _claimed_download(daemon, "A", 3)
    exit_times = []
    daemon._exit = lambda code: (exit_times.append(time.monotonic()), exits.append(code))
    blocker = sqlite3.connect(tmp_path / "lit.db", check_same_thread=False)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        gen = daemon._begin_work()
        with daemon._item_running("download", a):
            daemon._running_items[("download", a)] = time.monotonic() - 10
            started = time.monotonic()
            daemon._direct_batch_deadline_reached(gen, {"phase": "download"}, DEADLINE)
    finally:
        blocker.rollback()
        blocker.close()

    assert exits == [75]
    assert exit_times[0] - started < 1.6
    assert _download_row(tmp_path, a)[0] == "in_progress"
    assert any(line.startswith("Watchdog: hung items not recorded (") for line in lines)
    assert _heartbeat_status(tmp_path, daemon) != "error"


def test_a_skipped_hung_write_on_lock_timeout_still_exits(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "download")
    monkeypatch.setattr(mod, "WATCHDOG_LOCK_TIMEOUT_SECONDS", 0.05)
    a = _claimed_download(daemon, "A", 3)
    held, release = threading.Event(), threading.Event()

    def hold():
        with daemon.heartbeat_lock:
            held.set()
            release.wait(5)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert held.wait(2)
    try:
        gen = daemon._begin_work()
        with daemon._item_running("download", a):
            clock[0] = DEADLINE
            daemon._direct_batch_deadline_reached(gen, {"phase": "download"}, DEADLINE)
    finally:
        release.set()
        holder.join(5)

    assert exits == [75]
    assert any(line.startswith("Watchdog: hung items not recorded (") for line in lines)
    assert _download_row(tmp_path, a)[0] == "in_progress"


# --- the raw download ----------------------------------------------------------------------------

def test_a_raw_download_without_checksum_fails_the_metadata_claim(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "enrich")
    wid = _insert_work(daemon.db, "Raw", year=2020, metadata_status="pending")
    cols = "download_status, download_claimed_by, download_attempt_count, download_error, file_path"
    before = daemon.db.conn.execute(f"SELECT {cols} FROM works WHERE id = ?", (wid,)).fetchone()
    staged = []

    def raw(enhancer, ref, download_dir, *, part_suffix=None):
        assert part_suffix
        ref["part_suffix"] = part_suffix
        path = tmp_path / f"raw.pdf{part_suffix}"
        path.write_bytes(b"%PDF")
        staged.append(path)
        return {"downloaded_file": str(path), "download_source": "mock"}, "", []

    monkeypatch.setattr(daemon, "_enrich_single", lambda entry, *a: (int(entry["id"]), None, {"status": "no_match"}))
    monkeypatch.setattr(mod, "download_ref", raw)
    daemon.do_enrich(None, 5, 2, {})

    assert _metadata_row(tmp_path, wid) == ("failed", "checksum_unavailable", None)
    assert daemon.db.conn.execute(f"SELECT {cols} FROM works WHERE id = ?", (wid,)).fetchone() == before
    assert staged and not any(p.exists() for p in staged)
    assert not (tmp_path / "raw.pdf").exists()
    assert sorted(p.name for p in tmp_path.glob("*.pdf*")) == []


# --- enrichment after the latch, enrich claim loss ------------------------------------------------

def _enrich_rows(daemon, n):
    return [_insert_work(daemon.db, f"P{i}", year=2020, metadata_status="pending") for i in range(n)]


def test_enrichment_leaves_the_batch_once_the_latch_is_observed(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "enrich")
    monkeypatch.setenv("RAG_FEEDER_ENRICH_WAVE_SIZE", "2")
    first, second = _enrich_rows(daemon, 2)
    second_running, latched = threading.Event(), threading.Event()

    def enrich_single(entry, *args):
        # Both items run when the watchdog latches; the second returns its enrichment afterwards.
        if int(entry["id"]) == first:
            assert second_running.wait(5)
            daemon._latch_running_items()
            latched.set()
            return first, None, {"status": "no_match"}
        second_running.set()
        assert latched.wait(5)
        return second, {"title": "Enriched", "doi": "10.1/enriched"}, {"status": "matched"}

    monkeypatch.setattr(daemon, "_enrich_single", enrich_single)
    result = daemon.do_enrich(None, 5, 2, {})

    assert result["promoted"] == 0 and result["queued"] == 0
    for wid in (first, second):
        row = daemon.db.get_entry_by_id("works", wid)
        assert (row["metadata_status"], row["metadata_claimed_by"]) == ("in_progress", daemon.worker_id)
        assert row["download_status"] != "queued" and row["title"] != "Enriched"


def _steal(tmp_path, wid, phase="metadata"):
    c = _connect(tmp_path)
    c.execute(f"UPDATE works SET {phase}_claimed_by = 'daemon:other:1:aaa' WHERE id = ?", (wid,))
    c.commit()
    c.close()


def test_a_lost_claim_at_the_promotion_reset_is_skipped_not_failed(tmp_path, monkeypatch):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "enrich")
    (wid,) = _enrich_rows(daemon, 1)

    def apply(work_id, enriched, *, claimed_by=None):
        _steal(tmp_path, work_id)
        return None, "boom"

    monkeypatch.setattr(daemon, "_enrich_single", lambda entry, *a: (int(entry["id"]), {"title": "T"}, {}))
    monkeypatch.setattr(daemon.db, "apply_enriched_metadata", apply)
    result = daemon.do_enrich(None, 5, 2, {})

    assert (result["failed"], result["skipped"], result["errors"]) == (0, 1, [])
    assert result["results"] == [{"pending_work_id": wid, "action": "claim_lost"}]
    assert _metadata_row(tmp_path, wid) == ("in_progress", None, "daemon:other:1:aaa")
    assert f"Work {wid}: enrich claim lost before its result was written; result discarded." in lines


@pytest.mark.parametrize("where", ["apply", "mark_failed", "raw"])
def test_every_enrich_claim_loss_counts_as_skipped(tmp_path, monkeypatch, where):
    mod, daemon, exits, clock, lines = _daemon(monkeypatch, tmp_path, "enrich")
    (wid,) = _enrich_rows(daemon, 1)

    def enrich_single(entry, *args):
        _steal(tmp_path, wid)
        return wid, ({"title": "T"} if where == "apply" else None), {"status": "no_match"}

    def raw(enhancer, ref, download_dir, *, part_suffix=None):
        if where != "raw":
            return None, "", []
        path = tmp_path / f"raw.pdf{part_suffix}"
        path.write_bytes(b"%PDF")
        return {"downloaded_file": str(path), "download_source": "mock"}, "abc", []

    monkeypatch.setattr(daemon, "_enrich_single", enrich_single)
    monkeypatch.setattr(mod, "download_ref", raw)
    result = daemon.do_enrich(None, 5, 2, {})

    assert (result["failed"], result["skipped"]) == (0, 1)
    assert [r["action"] for r in result["results"]] == ["claim_lost"]
    assert _metadata_row(tmp_path, wid) == ("in_progress", None, "daemon:other:1:aaa")
    assert sorted(p.name for p in tmp_path.glob("*.pdf*")) == []
