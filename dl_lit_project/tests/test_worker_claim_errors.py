"""A failed enrich or download claim is an error, not an empty queue (D11; PY-54).

The worker claims with ``raise_errors=True``: a job whose claim fails ends ``failed`` and a
direct cycle's failure reaches the main loop. When the job's failure cannot be recorded either,
the worker exits with 75 and start-up recovery requeues the job. Every exit goes through
``daemon._exit`` (``exits.append`` here).
"""
import json
import sqlite3

import pytest

from dl_lit.db_manager import DatabaseManager
from tests._worker_import import load_worker
from tests.test_cli_download_leases import _deny_next_begin


def _daemon(monkeypatch, tmp_path, *, job_timeout=30):
    mod = load_worker("dt_worker_claim_errors", monkeypatch)
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


def _insert_running_job(tmp_path, worker_id, job_type, params):
    c = sqlite3.connect(tmp_path / "lit.db", timeout=5)
    cur = c.execute(
        "INSERT INTO pipeline_jobs (corpus_id, job_type, status, parameters_json, worker_id, started_at)"
        " VALUES (1, ?, 'running', ?, ?, datetime('now'))",
        (job_type, json.dumps(params), worker_id),
    )
    c.commit()
    job_id = cur.lastrowid
    c.close()
    return {"id": job_id, "job_type": job_type, "corpus_id": 1, "params": params}


def _job_row(tmp_path, job_id):
    c = sqlite3.connect(tmp_path / "lit.db", timeout=5)
    try:
        return c.execute(
            "SELECT status, result_json, worker_id FROM pipeline_jobs WHERE id = ?", (job_id,)
        ).fetchone()
    finally:
        c.close()


def _deny_claim(monkeypatch, daemon, claim):
    """Deny the BEGIN of the claim's own transaction only."""
    real = getattr(daemon.db, claim)
    monkeypatch.setattr(daemon.db, claim, lambda **kw: (_deny_next_begin(daemon.db), real(**kw))[1])


def test_claim_enrich_batch_raises_only_when_asked(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    try:
        _deny_next_begin(db)
        assert db.claim_enrich_batch(limit=1, corpus_id=None, claimed_by="w") == []  # the default
        _deny_next_begin(db)
        with pytest.raises(sqlite3.Error):
            db.claim_enrich_batch(limit=1, corpus_id=None, claimed_by="w", raise_errors=True)
        assert not db.conn.in_transaction
    finally:
        db.close_connection()


@pytest.mark.parametrize("job_type, params, claim", [
    ("enrich", {"limit": 1}, "claim_enrich_batch"),
    ("download", {"batchSize": 1}, "claim_download_batch"),
    ("pipeline_tick", {"config": {"promoteBatchSize": 1}}, "claim_enrich_batch"),
])
def test_a_failed_claim_fails_the_job(monkeypatch, tmp_path, job_type, params, claim):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    job = _insert_running_job(tmp_path, daemon.worker_id, job_type, params)
    _deny_claim(monkeypatch, daemon, claim)

    daemon._process_job(job, generation=daemon._begin_work())

    status, result_json, _ = _job_row(tmp_path, job["id"])
    assert status == "failed"
    assert "not authorized" in json.loads(result_json)["error"]
    assert exits == []


def test_a_failure_that_cannot_be_recorded_restarts_the_worker_and_requeues_the_job(monkeypatch, tmp_path):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    lines = []
    monkeypatch.setattr(mod, "log", lines.append)
    job = _insert_running_job(tmp_path, daemon.worker_id, "enrich", {"limit": 1})
    _deny_claim(monkeypatch, daemon, "claim_enrich_batch")
    monkeypatch.setattr(
        daemon,
        "mark_job_failed",
        lambda *a: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked")),
    )

    daemon._process_job(job, generation=daemon._begin_work())

    assert (
        f"Job {job['id']}: the failure could not be recorded (database is locked); "
        "exiting with code 75 so start-up recovery requeues it."
    ) in lines
    assert not any("failure not recorded because" in line for line in lines)
    assert exits == [75]
    status, _, worker_id = _job_row(tmp_path, job["id"])
    assert (status, worker_id) == ("running", daemon.worker_id)

    # Docker restarts the worker: start-up recovery's predecessor rule requeues the job.
    mod.PipelineDaemon(str(tmp_path / "lit.db"), role="enrich")
    status, _, worker_id = _job_row(tmp_path, job["id"])
    assert (status, worker_id) == ("pending", None)


def test_a_failed_claim_in_a_direct_cycle_reaches_the_loop(monkeypatch, tmp_path):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    _deny_claim(monkeypatch, daemon, "claim_enrich_batch")

    with pytest.raises(RuntimeError, match=r"^enrich cycle failed: "):
        daemon._run_direct_cycles()
    assert exits == []


# --- final fix (D11): only a claim failure is replayed; every terminal write is retried -------

def _locked(*a):
    raise sqlite3.OperationalError("database is locked")


def _scraper_job(monkeypatch, tmp_path, daemon):
    """A scraper run that commits one artifact row, and its running job."""
    from dl_lit import scraper_lab

    c = sqlite3.connect(tmp_path / "lit.db", timeout=5)
    scraper_lab.ensure_scraper_schema(c)
    c.execute("INSERT INTO scraper_runs (id, source_type, status, query_json) VALUES (7, 'eurlex', 'queued', '{}')")
    c.commit()
    c.close()
    monkeypatch.setattr(scraper_lab, "scrape_eurlex", lambda query: [{"title": "No link"}])
    params = {"run_id": 7, "artifacts_root": str(tmp_path / "artifacts")}
    return _insert_running_job(tmp_path, daemon.worker_id, "scrape_eurlex", params)


def _artifact_count(tmp_path):
    c = sqlite3.connect(tmp_path / "lit.db", timeout=5)
    try:
        return c.execute("SELECT COUNT(*) FROM scraper_artifacts WHERE run_id = 7").fetchone()[0]
    finally:
        c.close()


def test_a_job_that_did_work_and_cannot_record_its_end_is_not_replayed(monkeypatch, tmp_path):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    daemon.TERMINAL_WRITE_BACKOFF_SECONDS = (0, 0)
    lines = []
    monkeypatch.setattr(mod, "log", lines.append)
    job = _scraper_job(monkeypatch, tmp_path, daemon)
    attempts = []
    monkeypatch.setattr(daemon, "mark_job_completed", lambda *a: (attempts.append("completed"), _locked())[1])
    monkeypatch.setattr(daemon, "mark_job_failed", lambda *a: (attempts.append("failed"), _locked())[1])

    daemon._process_job(job, generation=daemon._begin_work())

    assert attempts == ["completed"] * 3 + ["failed"] * 3
    assert _artifact_count(tmp_path) == 1
    assert exits == []  # no restart, so start-up recovery does not run the scraper again
    assert (
        f"Job {job['id']}: its end could not be recorded (database is locked); "
        "it stays running until start-up recovery after the next restart."
    ) in lines
    assert not any("exiting with code 75" in line for line in lines)
    status, _, worker_id = _job_row(tmp_path, job["id"])
    assert (status, worker_id) == ("running", daemon.worker_id)


def test_a_failed_claim_whose_failure_write_is_retried_still_restarts(monkeypatch, tmp_path):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    daemon.TERMINAL_WRITE_BACKOFF_SECONDS = (0, 0)
    job = _insert_running_job(tmp_path, daemon.worker_id, "enrich", {"limit": 1})
    _deny_claim(monkeypatch, daemon, "claim_enrich_batch")
    attempts = []
    monkeypatch.setattr(daemon, "mark_job_failed", lambda *a: (attempts.append(1), _locked())[1])

    daemon._process_job(job, generation=daemon._begin_work())

    assert len(attempts) == 3
    assert exits == [75]


def test_a_lock_that_clears_on_the_second_attempt_records_the_completion(monkeypatch, tmp_path):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    daemon.TERMINAL_WRITE_BACKOFF_SECONDS = (0, 0)
    job = _scraper_job(monkeypatch, tmp_path, daemon)
    real = daemon.mark_job_completed
    calls = []

    def flaky(*a):
        calls.append(1)
        if len(calls) == 1:
            _locked()
        return real(*a)

    monkeypatch.setattr(daemon, "mark_job_completed", flaky)

    daemon._process_job(job, generation=daemon._begin_work())

    assert len(calls) == 2
    status, result_json, _ = _job_row(tmp_path, job["id"])
    assert status == "completed"
    assert json.loads(result_json)["run_id"] == 7
    assert exits == []
    assert _artifact_count(tmp_path) == 1


def test_a_lock_that_clears_on_the_second_attempt_records_the_failure(monkeypatch, tmp_path):
    mod, daemon, exits = _daemon(monkeypatch, tmp_path)
    daemon.TERMINAL_WRITE_BACKOFF_SECONDS = (0, 0)
    job = _insert_running_job(tmp_path, daemon.worker_id, "enrich", {"limit": 1})
    _deny_claim(monkeypatch, daemon, "claim_enrich_batch")
    real = daemon.mark_job_failed
    calls = []

    def flaky(*a):
        calls.append(1)
        if len(calls) == 1:
            _locked()
        return real(*a)

    monkeypatch.setattr(daemon, "mark_job_failed", flaky)

    daemon._process_job(job, generation=daemon._begin_work())

    assert len(calls) == 2
    assert _job_row(tmp_path, job["id"])[0] == "failed"
    assert exits == []
