"""Heartbeat threads survive any error, and the job/batch watchdog fails timed-out work only after
winning ownership, then exits the worker with 75 (D28, D29; PY-48, PY-49).

Every exit goes through ``daemon._exit`` (``exits.append`` here), and every thread or loop is
bounded by a timeout: an unguarded loop test was once OOM-killed.
"""
import json
import sqlite3
import threading
import time

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
        calls.append(1)
        return real_recover(self, *args, **kwargs)

    monkeypatch.setattr(mod.PipelineDaemon, "_recover_stale_running_jobs", spy)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RAG_FEEDER_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.delenv("RAG_FEEDER_LLM_PROVIDER", raising=False)
    monkeypatch.setattr(mod.PipelineDaemon, "_build_enhancer", lambda self, *a: object())
    daemon = mod.PipelineDaemon(str(tmp_path / "lit.db"), role="enrich")
    assert calls == [1]

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
    assert len(calls) == 2
