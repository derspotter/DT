import sqlite3
from types import SimpleNamespace

import pytest

from tests._worker_import import load_worker

WORKER_MODULE = load_worker("dt_pipeline_worker")
PipelineDaemon = WORKER_MODULE.PipelineDaemon


def make_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE pipeline_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            corpus_id INTEGER,
            job_type TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            parameters_json TEXT,
            result_json TEXT,
            worker_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            finished_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daemon_heartbeat (
            daemon_name TEXT PRIMARY KEY,
            worker_id TEXT,
            status TEXT,
            details_json TEXT,
            last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    return conn


def make_daemon(conn, worker_id="worker-1", timeout_seconds=300):
    daemon = PipelineDaemon.__new__(PipelineDaemon)
    daemon.db = SimpleNamespace(conn=conn)
    daemon.worker_id = worker_id
    daemon.job_timeout_seconds = timeout_seconds
    daemon.role = "all"
    daemon.daemon_name = "pipeline"
    return daemon


def test_fetch_next_job_claims_one_pending_job_with_worker_id():
    conn = make_conn()
    conn.execute(
        "INSERT INTO pipeline_jobs (corpus_id, job_type, status, parameters_json) VALUES (?,?,?,?)",
        (7, "download", "pending", '{"batchSize": 10}'),
    )
    conn.commit()

    daemon = make_daemon(conn, worker_id="worker-a")
    claimed = daemon.fetch_next_job()

    assert claimed["job_type"] == "download"
    row = conn.execute("SELECT status, worker_id FROM pipeline_jobs WHERE id = ?", (claimed["id"],)).fetchone()
    assert row == ("running", "worker-a")
    assert daemon.fetch_next_job() is None


def test_mark_job_completion_is_scoped_to_claiming_worker():
    conn = make_conn()
    conn.execute(
        "INSERT INTO pipeline_jobs (corpus_id, job_type, status, parameters_json) VALUES (?,?,?,?)",
        (1, "enrich", "pending", '{"limit": 5}'),
    )
    conn.commit()

    worker_a = make_daemon(conn, worker_id="worker-a")
    worker_b = make_daemon(conn, worker_id="worker-b")
    claimed = worker_a.fetch_next_job()

    assert worker_b.mark_job_completed(claimed["id"], {"ok": True}) is False
    assert worker_a.mark_job_completed(claimed["id"], {"ok": True}) is True

    row = conn.execute("SELECT status, worker_id FROM pipeline_jobs WHERE id = ?", (claimed["id"],)).fetchone()
    assert row == ("completed", "worker-a")


def test_recover_stale_running_jobs_only_requeues_expired_work():
    conn = make_conn()
    conn.execute(
        """
        INSERT INTO pipeline_jobs (corpus_id, job_type, status, worker_id, started_at)
        VALUES
          (1, 'download', 'running', 'worker-old', datetime('now', '-900 seconds')),
          (2, 'download', 'running', 'worker-live', datetime('now', '-30 seconds')),
          (3, 'enrich', 'running', 'worker-unknown', NULL)
        """
    )
    # worker-live is alive: it has a fresh heartbeat. worker-old timed out,
    # worker-unknown has no heartbeat at all.
    conn.execute(
        "INSERT INTO daemon_heartbeat (daemon_name, worker_id, status) VALUES ('download', 'worker-live', 'running')"
    )
    # worker-old is alive too (fresh heartbeat): only the timeout rule may requeue its job.
    conn.execute(
        "INSERT INTO daemon_heartbeat (daemon_name, worker_id, status) VALUES ('download-2', 'worker-old', 'running')"
    )
    conn.commit()

    daemon = make_daemon(conn, timeout_seconds=300)
    daemon._recover_stale_running_jobs()

    rows = conn.execute(
        "SELECT corpus_id, status, worker_id FROM pipeline_jobs ORDER BY corpus_id"
    ).fetchall()
    assert rows == [
        (1, "pending", None),
        (2, "running", "worker-live"),
        (3, "pending", None),
    ]


def test_do_mark_returns_the_constant_shape_without_querying(monkeypatch):
    conn = make_conn()
    daemon = make_daemon(conn)

    def forbidden():
        raise AssertionError("do_mark must not query")

    monkeypatch.setattr(daemon.db, "conn", SimpleNamespace(cursor=forbidden))
    assert daemon.do_mark(None, 10) == {"requested": 10, "marked": 0, "available": None, "ids": []}


# --- one run() loop for every role (spec D8) ---------------------------------------------

def _loop_daemon(role, monkeypatch, tmp_path):
    worker = WORKER_MODULE
    monkeypatch.setenv("RAG_FEEDER_LOG_DIR", str(tmp_path))
    daemon = worker.PipelineDaemon.__new__(worker.PipelineDaemon)
    daemon.role = role
    daemon.running = True
    daemon.heartbeat_interval_seconds = 3600
    events = []
    jobs = [{"id": 1, "job_type": "enrich", "params": {}}]
    direct_methods = {"enrich": "_run_direct_enrich_cycle", "download": "_run_direct_download_cycle"}
    expected_direct = ["enrich", "download"] if role == "all" else [role]

    daemon._install_signal_handlers = lambda: None
    daemon._heartbeat = lambda status, details=None, force=False: events.append(("heartbeat", status, (details or {}).get("state")))
    daemon.refresh_app_settings = lambda: events.append(("refresh",))

    def fetch_next_job():
        events.append(("fetch",))
        if sum(1 for e in events if e == ("fetch",)) >= 5:  # a loop that never ends the test
            daemon.running = False
        return jobs.pop(0) if jobs else None

    daemon.fetch_next_job = fetch_next_job
    daemon._process_job = lambda job: events.append(("job", job["id"]))

    def make_cycle(phase):
        def cycle():
            events.append(("direct", phase))
            if phase == expected_direct[-1]:
                daemon.running = False
            return {"processed": 1, "promoted": 1}
        return cycle

    for phase, name in direct_methods.items():
        setattr(daemon, name, make_cycle(phase))

    started = []
    real_start = daemon._start_direct_batch_heartbeat

    def start(details):
        started.append(details["phase"])
        return real_start(details)

    daemon._start_direct_batch_heartbeat = start
    def sleep(seconds):
        events.append(("sleep", seconds))
        if sum(1 for e in events if e[0] == "sleep") >= 5:  # a loop that never ends the test
            daemon.running = False

    monkeypatch.setattr(worker.time, "sleep", sleep)
    return daemon, events, started


@pytest.mark.parametrize("role", ["all", "enrich", "download"])
def test_one_loop_processes_a_job_then_a_direct_cycle(monkeypatch, tmp_path, role):
    daemon, events, started = _loop_daemon(role, monkeypatch, tmp_path)
    daemon.run()

    phases = ["enrich", "download"] if role == "all" else [role]
    assert events == [
        ("heartbeat", "idle", "polling"),
        ("refresh",),
        ("fetch",),
        ("job", 1),
        ("heartbeat", "idle", "polling"),
        ("refresh",),
        ("fetch",),
        *[("direct", phase) for phase in phases],
        ("heartbeat", "running", "processed_batch"),
    ]
    assert started == phases


def test_idle_loop_sleeps_five_seconds(monkeypatch, tmp_path):
    daemon, events, _ = _loop_daemon("all", monkeypatch, tmp_path)
    daemon._run_direct_enrich_cycle = lambda: None

    def idle_download():
        daemon.running = False
        return None

    daemon._run_direct_download_cycle = idle_download
    daemon.fetch_next_job = lambda: events.append(("fetch",)) or None
    daemon.run()
    assert events[-1] == ("sleep", 5)


def test_loop_exception_and_keyboard_interrupt_branches(monkeypatch, tmp_path):
    daemon, events, _ = _loop_daemon("enrich", monkeypatch, tmp_path)
    calls = {"n": 0}

    def fetch():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        raise KeyboardInterrupt

    daemon.fetch_next_job = fetch
    daemon._release_own_running_jobs = lambda: events.append(("released",))
    daemon.run()
    assert ("heartbeat", "error", "loop_exception") in events
    assert ("sleep", 10) in events
    assert events[-2:] == [("released",), ("heartbeat", "stopped", "keyboard_interrupt")]
    assert daemon.running is False
