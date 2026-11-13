"""Heartbeat threads survive any error, and the job/batch watchdog fails timed-out work only after
winning ownership, then exits the worker with 75 (D28, D29; PY-48, PY-49).

Every exit goes through ``daemon._exit`` (``exits.append`` here), and every thread or loop is
bounded by a timeout: an unguarded loop test was once OOM-killed.
"""
import json
import sqlite3
import threading
import time

import pytest

from tests._worker_import import load_worker

WAIT = 0.02


def _poll(cond, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(WAIT)
    return bool(cond())


def _daemon(monkeypatch, tmp_path, *, job_timeout=0.3):
    mod = load_worker("dt_worker_watchdog", monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RAG_FEEDER_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.delenv("RAG_FEEDER_LLM_PROVIDER", raising=False)
    monkeypatch.setattr(mod.PipelineDaemon, "_build_enhancer", lambda self, *a: object())
    daemon = mod.PipelineDaemon(str(tmp_path / "lit.db"), role="enrich")
    daemon.heartbeat_interval_seconds = 0.05
    daemon.job_timeout_seconds = job_timeout
    exits = []
    daemon._exit = exits.append
    return mod, daemon, exits


def _connect(tmp_path):
    return sqlite3.connect(tmp_path / "lit.db", timeout=5)


def _insert_job(tmp_path, status, worker_id):
    c = _connect(tmp_path)
    cur = c.execute(
        "INSERT INTO pipeline_jobs (job_type, status, worker_id, started_at) VALUES ('enrich', ?, ?, "
        + ("datetime('now')" if status == "running" else "NULL")
        + ")",
        (status, worker_id),
    )
    c.commit()
    job_id = cur.lastrowid
    c.close()
    return {"id": job_id, "job_type": "enrich", "corpus_id": None, "params": {}}


def _job_row(tmp_path, job_id):
    c = _connect(tmp_path)
    row = c.execute("SELECT status, worker_id, result_json FROM pipeline_jobs WHERE id = ?", (job_id,)).fetchone()
    c.close()
    return row


def _heartbeat_row(tmp_path, daemon):
    c = _connect(tmp_path)
    row = c.execute(
        "SELECT status, details_json FROM daemon_heartbeat WHERE daemon_name = ?", (daemon.daemon_name,)
    ).fetchone()
    c.close()
    return row[0], json.loads(row[1] or "{}")


def _stop(handle):
    stop, thread = handle
    stop.set()
    thread.join(5)
    assert not thread.is_alive()


class _ConnProxy:
    """Delegates to a real sqlite3 connection (whose attributes cannot be patched)."""

    def __init__(self, conn):
        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._conn, name)


# --- D28: heartbeat threads survive any error --------------------------------------------------

def test_beat_thread_survives_database_error(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path, job_timeout=60)
    calls = {"n": 0}

    class Malformed(_ConnProxy):
        def cursor(self):
            calls["n"] += 1
            if calls["n"] == 1:
                raise sqlite3.DatabaseError("database disk image is malformed")
            return self._conn.cursor()

    daemon.heartbeat_conn = Malformed(daemon.heartbeat_conn)
    job = _insert_job(tmp_path, "running", daemon.worker_id)
    gen = daemon._begin_work()
    handle = daemon._start_job_heartbeat(job, generation=gen)
    try:
        time.sleep(0.3)
        assert calls["n"] >= 2
        assert handle[1].is_alive()
        status, details = _heartbeat_row(tmp_path, daemon)
        assert status == "running" and details["state"] == "running_job"
    finally:
        _stop(handle)
    assert exits == []


# --- D29: job watchdog -------------------------------------------------------------------------

def test_job_past_deadline_is_failed_and_worker_exits(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    job = _insert_job(tmp_path, "running", daemon.worker_id)
    gen = daemon._begin_work()
    handle = daemon._start_job_heartbeat(job, generation=gen)
    try:
        assert _poll(lambda: exits == [mod.JOB_TIMEOUT_EXIT_CODE], 3)
        assert exits == [75]
        status, _, result_json = _job_row(tmp_path, job["id"])
        assert status == "failed"
        error = json.loads(result_json)["error"]
        assert error.startswith("timed out after") and error.endswith("; worker restarted")
        hb_status, details = _heartbeat_row(tmp_path, daemon)
        assert hb_status == "error" and details["state"] == "job_timeout"
        assert daemon._terminal is True
        assert daemon._begin_work() is None
    finally:
        _stop(handle)
    assert exits == [75]


def test_completed_job_wins_and_no_exit(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    job = _insert_job(tmp_path, "running", daemon.worker_id)
    gen = daemon._begin_work()
    handle = daemon._start_job_heartbeat(job, generation=gen)
    try:
        assert daemon.mark_job_completed(job["id"], {"ok": True}) is True
        time.sleep(1.0)  # well past the 0.3 s deadline; the generation is still open
        assert exits == []
        assert _job_row(tmp_path, job["id"])[0] == "completed"
        assert daemon._terminal is False
    finally:
        _stop(handle)


def test_stale_callback_does_not_touch_the_next_job(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path, job_timeout=60)
    callbacks = {}
    real_start = daemon._start_heartbeat_thread

    def spy(payload, **kwargs):
        callbacks[kwargs["generation"]] = kwargs["on_deadline"]
        return real_start(payload, **kwargs)

    daemon._start_heartbeat_thread = spy
    job_a = _insert_job(tmp_path, "running", daemon.worker_id)
    gen_a = daemon._begin_work()
    handle_a = daemon._start_job_heartbeat(job_a, generation=gen_a)
    daemon._end_work(gen_a)  # job A finished; its thread is left running (teardown join timed out)
    job_b = _insert_job(tmp_path, "running", daemon.worker_id)
    gen_b = daemon._begin_work()
    handle_b = daemon._start_job_heartbeat(job_b, generation=gen_b)
    try:
        assert gen_b != gen_a
        callbacks[gen_a](gen_a)
        assert exits == []
        assert _job_row(tmp_path, job_b["id"])[0] == "running"
        assert _job_row(tmp_path, job_a["id"])[0] == "running"
        assert daemon._terminal is False
    finally:
        _stop(handle_a)
        _stop(handle_b)


def test_watchdog_waits_for_the_write_lock_and_exits_only_after_winning(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    lines = []
    real_log = mod.log
    monkeypatch.setattr(mod, "log", lambda msg: (lines.append(msg), real_log(msg)))
    job = _insert_job(tmp_path, "running", daemon.worker_id)
    holder = sqlite3.connect(tmp_path / "lit.db", check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE")
    gen = daemon._begin_work()
    handle = daemon._start_job_heartbeat(job, generation=gen)
    try:
        time.sleep(2.5)
        assert exits == []
        assert _job_row(tmp_path, job["id"])[0] == "running"
        # Each beat also waits out the regular heartbeat's 3 s busy timeout, so the first
        # failed ownership attempt is logged within about 4 s.
        assert _poll(lambda: any(f"job {job['id']} timed out but could not be marked failed" in line for line in lines), 6)
        assert exits == []
        assert daemon._terminal is False
        holder.rollback()
        assert _job_row(tmp_path, job["id"])[0] == "running"
        assert _poll(lambda: exits == [75], 5)
        assert _job_row(tmp_path, job["id"])[0] == "failed"
    finally:
        if holder.in_transaction:
            holder.rollback()
        holder.close()
        _stop(handle)


def test_win_latches_terminal_state_until_exit(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    gate = threading.Event()
    daemon._after_watchdog_win = lambda: gate.wait(10)
    job_a = _insert_job(tmp_path, "running", daemon.worker_id)
    gen_a = daemon._begin_work()
    handle = daemon._start_job_heartbeat(job_a, generation=gen_a)
    runner = None
    try:
        assert _poll(lambda: daemon._terminal, 3)
        assert exits == []  # the callback is suspended right after the latch
        assert _job_row(tmp_path, job_a["id"])[0] == "failed"
        daemon._end_work(gen_a)  # job A's main path finishes meanwhile

        job_b = _insert_job(tmp_path, "pending", None)
        c = _connect(tmp_path)
        c.execute("INSERT INTO works (id, title, metadata_status) VALUES (900, 'w', 'pending')")
        c.commit()
        c.close()

        fetched, begun = [], []
        daemon.fetch_next_job = lambda: fetched.append(1)
        real_begin = daemon._begin_work
        daemon._begin_work = lambda: begun.append(real_begin()) or begun[-1]
        runner = threading.Thread(target=daemon.run, daemon=True)
        runner.start()
        assert _poll(lambda: begun, 3)
        time.sleep(0.6)
        assert begun == [None]
        assert fetched == []
        assert _job_row(tmp_path, job_b["id"])[:2] == ("pending", None)
        c = _connect(tmp_path)
        assert c.execute("SELECT metadata_status, metadata_claimed_by FROM works WHERE id = 900").fetchone() == ("pending", None)
        c.close()

        gate.set()
        assert _poll(lambda: exits == [75], 4)
    finally:
        gate.set()
        daemon.running = False
        if runner is not None:
            runner.join(3)
            assert not runner.is_alive()
        _stop(handle)


def test_watchdog_heartbeat_lock_wait_is_bounded(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    lines = []
    monkeypatch.setattr(mod, "log", lines.append)
    job = _insert_job(tmp_path, "running", daemon.worker_id)
    held, release = threading.Event(), threading.Event()

    def hold():
        with daemon.heartbeat_lock:
            held.set()
            release.wait(5)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert held.wait(2)
    gen = daemon._begin_work()
    handle = daemon._start_job_heartbeat(job, generation=gen)
    try:
        assert _poll(lambda: any("could not be marked failed" in line for line in lines), 3)
        time.sleep(1.0)
        assert exits == []
        assert _job_row(tmp_path, job["id"])[0] == "running"
        release.set()
        assert _poll(lambda: exits == [75], 4)
        assert _job_row(tmp_path, job["id"])[0] == "failed"
    finally:
        release.set()
        holder.join(6)
        _stop(handle)


def test_terminal_reporting_is_bounded(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    blocker = sqlite3.connect(tmp_path / "lit.db", check_same_thread=False)
    marks = {}
    exit_times = []

    class LockAfterWin(_ConnProxy):
        pending = False

        def execute(self, sql, *args):
            if "UPDATE pipeline_jobs" in sql:
                LockAfterWin.pending = True
            return self._conn.execute(sql, *args)

        def commit(self):
            self._conn.commit()
            if LockAfterWin.pending and "win" not in marks:
                marks["win"] = time.monotonic()
                blocker.execute("BEGIN IMMEDIATE")

    daemon.heartbeat_conn = LockAfterWin(daemon.heartbeat_conn)
    daemon._exit = lambda code: (exit_times.append(time.monotonic()), exits.append(code))
    job = _insert_job(tmp_path, "running", daemon.worker_id)
    gen = daemon._begin_work()
    handle = daemon._start_job_heartbeat(job, generation=gen)
    try:
        assert _poll(lambda: exits == [75], 4)
        assert "win" in marks
        assert exit_times[0] - marks["win"] < 2.5
        assert _job_row(tmp_path, job["id"])[0] == "failed"
    finally:
        if blocker.in_transaction:
            blocker.rollback()
        blocker.close()
        _stop(handle)


# --- D29: direct batch watchdog -----------------------------------------------------------------

def test_direct_batch_deadline_exits(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path, job_timeout=60)
    daemon._direct_batch_timeout_seconds = lambda: 0.3
    gen = daemon._begin_work()
    handle = daemon._start_direct_batch_heartbeat({"phase": "enrich"}, generation=gen)
    try:
        assert _poll(lambda: exits == [75], 3)
        status, details = _heartbeat_row(tmp_path, daemon)
        assert status == "error" and details["state"] == "direct_batch_timeout"
        assert daemon._begin_work() is None
    finally:
        _stop(handle)


def test_finished_direct_batch_never_exits(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path, job_timeout=60)
    daemon._direct_batch_timeout_seconds = lambda: 0.3
    gen = daemon._begin_work()
    handle = daemon._start_direct_batch_heartbeat({"phase": "enrich"}, generation=gen)
    try:
        daemon._end_work(gen)
        time.sleep(0.8)
        assert exits == [] and daemon._terminal is False
    finally:
        _stop(handle)


def test_direct_batch_timeout_reads_env_with_minimum(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path, job_timeout=900)
    monkeypatch.delenv("RAG_FEEDER_DIRECT_BATCH_TIMEOUT", raising=False)
    assert daemon._direct_batch_timeout_seconds() == 900
    monkeypatch.setenv("RAG_FEEDER_DIRECT_BATCH_TIMEOUT", "5")
    assert daemon._direct_batch_timeout_seconds() == 60
    monkeypatch.setenv("RAG_FEEDER_DIRECT_BATCH_TIMEOUT", "120")
    assert daemon._direct_batch_timeout_seconds() == 120


# --- restart and periodic recovery -------------------------------------------------------------

def test_restart_recovery_releases_every_claim_of_the_dead_worker(tmp_path, monkeypatch):
    mod, old, _ = _daemon(monkeypatch, tmp_path)
    c = _connect(tmp_path)
    c.executemany(
        "INSERT INTO works (id, title, metadata_status, metadata_claimed_by, metadata_claimed_at, metadata_lease_expires_at)"
        " VALUES (?, 'w', 'in_progress', ?, 1, 9999999999)",
        [(i, old.worker_id) for i in (1, 2, 3)],
    )
    c.commit()
    c.close()
    job = _insert_job(tmp_path, "running", old.worker_id)
    assert _heartbeat_row(tmp_path, old)[0] == "starting"

    mod2, new, _ = _daemon(monkeypatch, tmp_path)
    assert new.daemon_name == old.daemon_name and new.worker_id != old.worker_id
    c = _connect(tmp_path)
    rows = c.execute(
        "SELECT metadata_status, metadata_claimed_by, metadata_claimed_at, metadata_lease_expires_at FROM works ORDER BY id"
    ).fetchall()
    c.close()
    assert rows == [("pending", None, None, None)] * 3
    assert _job_row(tmp_path, job["id"])[:2] == ("pending", None)


def test_run_recovers_stale_jobs_every_60s(tmp_path, monkeypatch):
    mod = load_worker("dt_worker_watchdog", monkeypatch)
    calls = []
    real_recover = mod.PipelineDaemon._recover_stale_running_jobs

    def spy(self, *args, **kwargs):
        calls.append(kwargs.get("periodic", False))
        return real_recover(self, *args, **kwargs)

    monkeypatch.setattr(mod.PipelineDaemon, "_recover_stale_running_jobs", spy)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RAG_FEEDER_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.delenv("RAG_FEEDER_LLM_PROVIDER", raising=False)
    monkeypatch.setattr(mod.PipelineDaemon, "_build_enhancer", lambda self, *a: object())
    daemon = mod.PipelineDaemon(str(tmp_path / "lit.db"), role="enrich")
    assert calls == [False]  # start-up keeps the full rule

    base = time.time()
    offset = [0.0]
    fetches = []

    def fetch():
        fetches.append(1)
        offset[0] += 61
        if len(fetches) >= 2:
            daemon.running = False
        return None

    daemon.fetch_next_job = fetch
    daemon._run_direct_enrich_cycle = lambda: None
    monkeypatch.setattr(mod.time, "time", lambda: base + offset[0])
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    daemon.run()
    assert len(fetches) == 2
    assert calls == [False, True]  # the 60 s call reclaims from dead owners only


# --- fix round 1: periodic recovery reclaims only from owners that are positively dead ---------

def _cross_worker_setup(monkeypatch, tmp_path, owner_status, owner_last_seen):
    """Two overlapping claimers: owner A (role all, daemon_name 'pipeline') holds an overdue
    enrich job and a work claim; B (role enrich) runs its periodic recovery."""
    mod = load_worker("dt_worker_watchdog", monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RAG_FEEDER_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.delenv("RAG_FEEDER_LLM_PROVIDER", raising=False)
    monkeypatch.setattr(mod.PipelineDaemon, "_build_enhancer", lambda self, *a: object())
    owner = mod.PipelineDaemon(str(tmp_path / "lit.db"), role="all")
    other = mod.PipelineDaemon(str(tmp_path / "lit.db"), role="enrich")
    other._exit = lambda code: None
    c = _connect(tmp_path)
    cur = c.execute(
        "INSERT INTO pipeline_jobs (job_type, status, worker_id, started_at)"
        " VALUES ('enrich', 'running', ?, datetime('now', '-2 hours'))",
        (owner.worker_id,),
    )
    job_id = cur.lastrowid
    c.execute(
        "INSERT INTO works (id, title, metadata_status, metadata_claimed_by, metadata_claimed_at, metadata_lease_expires_at)"
        " VALUES (77, 'w', 'in_progress', ?, 1, 9999999999)",
        (owner.worker_id,),
    )
    c.execute(
        f"UPDATE daemon_heartbeat SET status = ?, last_seen_at = datetime('now', '{owner_last_seen}') WHERE worker_id = ?",
        (owner_status, owner.worker_id),
    )
    c.commit()
    c.close()
    return mod, owner, other, job_id


def _run_other_once(mod, other, monkeypatch):
    """One loop iteration of B with recovery due; the real fetch_next_job claims whatever is pending."""
    claimed = []
    real_fetch = other.fetch_next_job

    def fetch():
        job = real_fetch()
        claimed.append(job and job["id"])
        other.running = False
        return job

    other.fetch_next_job = fetch
    other._process_job = lambda job, **kw: other._end_work(kw["generation"])
    other._run_direct_enrich_cycle = lambda: None
    other._last_recovery_at = time.time() - 61
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    other.run()
    return claimed


def _claim_row(tmp_path):
    c = _connect(tmp_path)
    row = c.execute("SELECT metadata_status, metadata_claimed_by FROM works WHERE id = 77").fetchone()
    c.close()
    return row



@pytest.mark.parametrize("last_seen", ["-1 seconds", "-1 hours"], ids=["fresh", "stale"])
def test_periodic_recovery_leaves_a_live_owners_overdue_job_running(tmp_path, monkeypatch, last_seen):
    # The owner's watchdog cannot win while its job holds the write lock; another worker's
    # periodic recovery must not hand the job out again (duplicate execution), whatever its age
    # and however stale the owner's heartbeat looks under write contention.
    mod, owner, other, job_id = _cross_worker_setup(monkeypatch, tmp_path, "running", last_seen)
    claimed = _run_other_once(mod, other, monkeypatch)
    assert claimed == [None]
    assert _job_row(tmp_path, job_id)[:2] == ("running", owner.worker_id)
    assert _claim_row(tmp_path) == ("in_progress", owner.worker_id)


def test_periodic_recovery_reclaims_from_a_stopped_owner(tmp_path, monkeypatch):
    mod, owner, other, job_id = _cross_worker_setup(monkeypatch, tmp_path, "stopped", "-1 seconds")
    claimed = _run_other_once(mod, other, monkeypatch)
    assert claimed == [job_id]
    assert _job_row(tmp_path, job_id)[:2] == ("running", other.worker_id)
    assert _claim_row(tmp_path) == ("pending", None)


def test_periodic_recovery_reclaims_from_an_owner_without_heartbeat_row(tmp_path, monkeypatch):
    # A non-daemon owner id (no host/pid to check) with no heartbeat row at all is dead. A daemon
    # id is checked by host and pid instead (final fix wave K-M5, tests below).
    mod, owner, other, job_id = _cross_worker_setup(monkeypatch, tmp_path, "running", "-1 seconds")
    c = _connect(tmp_path)
    c.execute("DELETE FROM daemon_heartbeat WHERE worker_id = ?", (owner.worker_id,))
    c.execute("UPDATE pipeline_jobs SET worker_id = 'legacy-worker' WHERE id = ?", (job_id,))
    c.execute("UPDATE works SET metadata_claimed_by = 'legacy-worker' WHERE id = 77")
    c.commit()
    c.close()
    other._recover_stale_running_jobs(periodic=True)
    assert _job_row(tmp_path, job_id)[:2] == ("pending", None)
    assert _claim_row(tmp_path) == ("pending", None)


def test_periodic_recovery_never_reclaims_this_workers_own_overdue_job(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path, job_timeout=60)
    c = _connect(tmp_path)
    cur = c.execute(
        "INSERT INTO pipeline_jobs (job_type, status, worker_id, started_at)"
        " VALUES ('enrich', 'running', ?, datetime('now', '-2 hours'))",
        (daemon.worker_id,),
    )
    job_id = cur.lastrowid
    c.execute(
        "UPDATE daemon_heartbeat SET last_seen_at = datetime('now', '-1 hours') WHERE worker_id = ?",
        (daemon.worker_id,),
    )
    c.commit()
    c.close()
    assert daemon._recover_stale_running_jobs(periodic=True) == 0
    assert _job_row(tmp_path, job_id)[:2] == ("running", daemon.worker_id)


# --- final fix wave K-I1: scraper jobs have no watchdog; recovery fails their scraper run -------

def _insert_typed_job(tmp_path, job_type, worker_id, params=None):
    c = _connect(tmp_path)
    cur = c.execute(
        "INSERT INTO pipeline_jobs (job_type, status, worker_id, started_at, parameters_json)"
        " VALUES (?, 'running', ?, datetime('now'), ?)",
        (job_type, worker_id, json.dumps(params or {})),
    )
    c.commit()
    job_id = cur.lastrowid
    c.close()
    return {"id": job_id, "job_type": job_type, "corpus_id": None, "params": params or {}}


@pytest.mark.parametrize("job_type", ["scrape_eurlex", "scrape_expert_groups"])
def test_scraper_jobs_are_not_watched(tmp_path, monkeypatch, job_type):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    job = _insert_typed_job(tmp_path, job_type, daemon.worker_id, {"run_id": 1})
    gen = daemon._begin_work()
    handle = daemon._start_job_heartbeat(job, generation=gen)
    try:
        time.sleep(1.0)  # well past the 0.3 s job timeout
        assert exits == []
        assert _job_row(tmp_path, job["id"])[0] == "running"
        assert daemon._terminal is False
    finally:
        _stop(handle)


def _scraper_run(tmp_path, run_id, status="running"):
    from dl_lit.scraper_lab import ensure_scraper_schema

    c = _connect(tmp_path)
    ensure_scraper_schema(c)
    c.execute("INSERT INTO scraper_runs (id, source_type, status) VALUES (?, 'eurlex', ?)", (run_id, status))
    c.commit()
    c.close()


def _scraper_run_row(tmp_path, run_id):
    c = _connect(tmp_path)
    row = c.execute("SELECT status, error, finished_at FROM scraper_runs WHERE id = ?", (run_id,)).fetchone()
    c.close()
    return row


def test_startup_recovery_fails_the_scraper_run_of_a_recovered_job(tmp_path, monkeypatch):
    mod, old, _ = _daemon(monkeypatch, tmp_path)
    _scraper_run(tmp_path, 5)
    _scraper_run(tmp_path, 6)  # its job is not recovered: it stays as it is
    job = _insert_typed_job(tmp_path, "scrape_eurlex", old.worker_id, {"run_id": 5})

    mod2, new, _ = _daemon(monkeypatch, tmp_path)
    assert _job_row(tmp_path, job["id"])[:2] == ("pending", None)
    status, error, finished_at = _scraper_run_row(tmp_path, 5)
    assert status == "failed" and "worker stopped" in error and finished_at
    assert _scraper_run_row(tmp_path, 6)[0] == "running"


def test_startup_recovery_without_scraper_tables_still_recovers(tmp_path, monkeypatch):
    mod, old, _ = _daemon(monkeypatch, tmp_path)
    job = _insert_typed_job(tmp_path, "scrape_eurlex", old.worker_id, {"run_id": 5})
    mod2, new, _ = _daemon(monkeypatch, tmp_path)
    assert _job_row(tmp_path, job["id"])[:2] == ("pending", None)


# --- final fix wave K-M5: periodic predecessor = same host and a dead pid ----------------------

def _dead_pid():
    import subprocess
    import sys as _sys

    proc = subprocess.Popen([_sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def _same_role_owner(monkeypatch, tmp_path, owner_id, *, owner_beat_last):
    """Another enrich worker `owner_id` holds a running job and a claim; `daemon` (enrich) runs
    its periodic recovery. owner_beat_last: the shared 'enrich' heartbeat row names the owner."""
    mod, daemon, exits = _daemon(monkeypatch, tmp_path, job_timeout=60)
    job = _insert_typed_job(tmp_path, "enrich", owner_id)
    c = _connect(tmp_path)
    c.execute(
        "INSERT INTO works (id, title, metadata_status, metadata_claimed_by, metadata_claimed_at, metadata_lease_expires_at)"
        " VALUES (77, 'w', 'in_progress', ?, 1, 9999999999)",
        (owner_id,),
    )
    if owner_beat_last:
        c.execute("UPDATE daemon_heartbeat SET worker_id = ? WHERE daemon_name = 'enrich'", (owner_id,))
    c.commit()
    c.close()
    return mod, daemon, job


@pytest.mark.parametrize("owner_beat_last", [True, False], ids=["row-names-owner", "row-names-us"])
def test_periodic_recovery_keeps_a_live_same_host_same_role_workers_job(tmp_path, monkeypatch, owner_beat_last):
    import os
    import socket

    owner_id = f"daemon:{socket.gethostname()}:{os.getppid()}:abc123"  # a live process on this host
    mod, daemon, job = _same_role_owner(monkeypatch, tmp_path, owner_id, owner_beat_last=owner_beat_last)
    assert daemon._recover_stale_running_jobs(periodic=True) == 0
    assert _job_row(tmp_path, job["id"])[:2] == ("running", owner_id)
    assert _claim_row(tmp_path) == ("in_progress", owner_id)


@pytest.mark.parametrize("owner_beat_last", [True, False], ids=["row-names-owner", "row-names-us"])
def test_periodic_recovery_never_reclaims_another_hosts_worker(tmp_path, monkeypatch, owner_beat_last):
    owner_id = f"daemon:some-other-host:{_dead_pid()}:abc123"
    mod, daemon, job = _same_role_owner(monkeypatch, tmp_path, owner_id, owner_beat_last=owner_beat_last)
    assert daemon._recover_stale_running_jobs(periodic=True) == 0
    assert _job_row(tmp_path, job["id"])[:2] == ("running", owner_id)
    assert _claim_row(tmp_path) == ("in_progress", owner_id)


def test_periodic_recovery_reclaims_a_same_host_predecessor_with_a_dead_pid(tmp_path, monkeypatch):
    import socket

    owner_id = f"daemon:{socket.gethostname()}:{_dead_pid()}:abc123"
    mod, daemon, job = _same_role_owner(monkeypatch, tmp_path, owner_id, owner_beat_last=True)
    assert daemon._recover_stale_running_jobs(periodic=True) == 1
    assert _job_row(tmp_path, job["id"])[:2] == ("pending", None)
    assert _claim_row(tmp_path) == ("pending", None)


# --- final fix wave S-M6, S-M7: watchdog log lines ----------------------------------------------

def _capture_log(mod, monkeypatch):
    lines = []
    real = mod.log
    monkeypatch.setattr(mod, "log", lambda msg: (lines.append(msg), real(msg)))
    return lines


def test_skipped_terminal_heartbeat_is_logged_on_lock_timeout(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    lines = _capture_log(mod, monkeypatch)
    monkeypatch.setattr(mod, "WATCHDOG_LOCK_TIMEOUT_SECONDS", 0.05)
    holder_ready, release = threading.Event(), threading.Event()

    def hold():
        with daemon.heartbeat_lock:
            holder_ready.set()
            release.wait(5)

    t = threading.Thread(target=hold, daemon=True)
    t.start()
    assert holder_ready.wait(2)
    try:
        daemon._finish_after_win({"state": "job_timeout"}, "Job 1 timed out")
    finally:
        release.set()
        t.join(5)
    assert exits == [75]
    assert any("terminal error heartbeat skipped" in line and "lock" in line for line in lines)


def test_skipped_terminal_heartbeat_is_logged_on_exception(tmp_path, monkeypatch):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    lines = _capture_log(mod, monkeypatch)

    class Broken(_ConnProxy):
        def execute(self, sql, *args):
            if "PRAGMA busy_timeout" in sql:
                raise sqlite3.OperationalError("disk I/O error")
            return self._conn.execute(sql, *args)

    daemon.heartbeat_conn = Broken(daemon.heartbeat_conn)
    daemon._finish_after_win({"state": "job_timeout"}, "Job 1 timed out")
    assert exits == [75]
    assert any("terminal error heartbeat skipped" in line and "disk I/O error" in line for line in lines)


@pytest.mark.parametrize("outcome", ["completed", "failed"])
def test_completion_after_a_watchdog_win_says_timed_out(tmp_path, monkeypatch, outcome):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path, job_timeout=60)
    lines = _capture_log(mod, monkeypatch)
    job = _insert_typed_job(tmp_path, "scrape_eurlex", daemon.worker_id, {"run_id": 1})

    def run_scraper(*args, **kwargs):
        # The watchdog wins while the job is still running.
        assert daemon._claim_timed_out_job(job["id"], "timed out after 60s; worker restarted") is True
        daemon._terminal = True
        if outcome == "failed":
            raise RuntimeError("late failure")
        return {"ok": True}

    monkeypatch.setattr(mod, "execute_scraper_job", run_scraper)
    gen = daemon._begin_work()
    daemon._process_job(job, generation=gen)
    assert _job_row(tmp_path, job["id"])[0] == "failed"
    assert any("timed out by the watchdog" in line for line in lines)
    assert not any("already cancelled" in line for line in lines)
