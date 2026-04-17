import importlib.util
import sqlite3
from pathlib import Path
from types import SimpleNamespace


WORKER_PATH = Path(__file__).resolve().parents[2] / "backend" / "scripts" / "daemon" / "worker.py"
SPEC = importlib.util.spec_from_file_location("dt_pipeline_worker", WORKER_PATH)
WORKER_MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(WORKER_MODULE)
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
    return conn


def make_daemon(conn, worker_id="worker-1", timeout_seconds=300):
    daemon = PipelineDaemon.__new__(PipelineDaemon)
    daemon.db = SimpleNamespace(conn=conn)
    daemon.worker_id = worker_id
    daemon.job_timeout_seconds = timeout_seconds
    daemon.role = "all"
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
