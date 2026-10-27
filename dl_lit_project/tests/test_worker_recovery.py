"""Abandoned-job recovery around container restarts (scripts/deploy.sh restarts workers routinely)."""
import importlib.util
import os
import signal
import sqlite3
import time
from pathlib import Path

import pytest

WORKER = Path(__file__).resolve().parents[2] / "backend" / "scripts" / "daemon" / "worker.py"


def _load(monkeypatch):
    monkeypatch.syspath_prepend(str(WORKER.parent))
    spec = importlib.util.spec_from_file_location("dt_worker_recovery", WORKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, corpus_id INTEGER, job_type TEXT NOT NULL,
    status TEXT DEFAULT 'pending', parameters_json TEXT, result_json TEXT, worker_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, started_at TIMESTAMP, finished_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS daemon_heartbeat (
    daemon_name TEXT PRIMARY KEY, worker_id TEXT, status TEXT, details_json TEXT,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
"""
NOW = "datetime('now')"  # every fixture timestamp is recent: only the new predicates may fire


def _seed(path, jobs, heartbeats):
    c = sqlite3.connect(path)
    c.executescript(SCHEMA)
    for job_type, worker_id in jobs:
        c.execute(
            f"INSERT INTO pipeline_jobs (job_type, status, worker_id, started_at) VALUES (?, 'running', ?, {NOW})",
            (job_type, worker_id),
        )
    for name, worker_id in heartbeats:
        c.execute(
            f"INSERT INTO daemon_heartbeat (daemon_name, worker_id, status, last_seen_at) VALUES (?, ?, 'running', {NOW})",
            (name, worker_id),
        )
    c.commit()
    c.close()


def _statuses(path):
    c = sqlite3.connect(path)
    rows = c.execute("SELECT job_type, worker_id, status FROM pipeline_jobs ORDER BY id").fetchall()
    c.close()
    return rows


def _daemon(monkeypatch, tmp_path, role):
    mod = _load(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("RAG_FEEDER_LLM_PROVIDER", raising=False)
    monkeypatch.setattr(mod.PipelineDaemon, "_build_enhancer", lambda self, *a: object())
    return mod.PipelineDaemon(str(tmp_path / "lit.db"), role=role)


def test_enrich_startup_requeues_its_predecessors_and_orphans_only(monkeypatch, tmp_path):
    db = tmp_path / "lit.db"
    _seed(
        db,
        jobs=[
            ("enrich", "daemon:box:1:aaa"),       # predecessor: same daemon_name row, fresh heartbeat, other id
            ("enrich", "daemon:elsewhere:7:bbb"), # no heartbeat at all
            ("download", "daemon:box:2:ccc"),     # other role: not ours to touch
            ("enrich", "daemon:pipe:3:ddd"),      # claimed by a live role-all daemon, fresh
        ],
        heartbeats=[
            ("enrich", "daemon:box:1:aaa"),
            ("download", "daemon:box:2:ccc"),
            ("pipeline", "daemon:pipe:3:ddd"),
        ],
    )
    _daemon(monkeypatch, tmp_path, "enrich")

    assert _statuses(db) == [
        ("enrich", None, "pending"),
        ("enrich", None, "pending"),
        ("download", "daemon:box:2:ccc", "running"),
        ("enrich", "daemon:pipe:3:ddd", "running"),
    ]


def test_download_startup_requeues_only_download_jobs(monkeypatch, tmp_path):
    db = tmp_path / "lit.db"
    _seed(
        db,
        jobs=[("download", "daemon:box:2:ccc"), ("enrich", "daemon:gone:1:eee")],
        heartbeats=[("download", "daemon:box:2:ccc")],
    )
    _daemon(monkeypatch, tmp_path, "download")
    assert _statuses(db) == [("download", None, "pending"), ("enrich", "daemon:gone:1:eee", "running")]


def test_release_own_running_jobs_on_shutdown(monkeypatch, tmp_path):
    db = tmp_path / "lit.db"
    _seed(db, jobs=[], heartbeats=[])
    daemon = _daemon(monkeypatch, tmp_path, "enrich")
    c = sqlite3.connect(db)
    c.execute("INSERT INTO pipeline_jobs (job_type, status, worker_id, started_at) VALUES ('enrich', 'running', ?, datetime('now'))", (daemon.worker_id,))
    c.execute("INSERT INTO pipeline_jobs (job_type, status, worker_id, started_at) VALUES ('enrich', 'running', 'daemon:other:5', datetime('now'))")
    c.commit()
    c.close()
    assert daemon._release_own_running_jobs() == 1
    assert _statuses(db) == [("enrich", None, "pending"), ("enrich", "daemon:other:5", "running")]


def test_worker_ids_differ_between_processes_on_the_same_host_and_pid(monkeypatch, tmp_path):
    a = _daemon(monkeypatch, tmp_path, "enrich")
    b = _daemon(monkeypatch, tmp_path, "enrich")
    assert a.worker_id != b.worker_id
    assert a.worker_id.startswith("daemon:")


def _claims(path):
    c = sqlite3.connect(path)
    rows = c.execute("SELECT id, metadata_status, metadata_claimed_by, download_status, download_claimed_by FROM works ORDER BY id").fetchall()
    c.close()
    return rows


def test_startup_recovery_releases_work_claims_of_dead_workers_for_its_role(monkeypatch, tmp_path):
    db = tmp_path / "lit.db"
    _seed(db, jobs=[], heartbeats=[("enrich", "daemon:box:1:aaa"), ("download", "daemon:box:2:ccc")])
    _daemon(monkeypatch, tmp_path, "enrich")  # creates the real works schema; nothing to recover yet
    c = sqlite3.connect(db)
    c.executemany(
        "INSERT INTO works (id, title, metadata_status, metadata_claimed_by, metadata_lease_expires_at, download_status, download_claimed_by, download_lease_expires_at) VALUES (?,'w',?,?,?,?,?,?)",
        [
            (1, "in_progress", "daemon:box:1:aaa", 9999999999, "not_requested", None, None),   # predecessor's claim -> pending
            (2, "in_progress", "daemon:ghost:9:fff", 9999999999, "not_requested", None, None), # no heartbeat -> pending
            (3, "matched", None, None, "in_progress", "daemon:box:2:ccc", 9999999999),        # download role's claim: not ours
            (4, "in_progress", "daemon:live:4:ggg", 9999999999, "not_requested", None, None),  # live worker -> untouched
        ],
    )
    c.execute("INSERT INTO daemon_heartbeat (daemon_name, worker_id, status, last_seen_at) VALUES ('pipeline', 'daemon:live:4:ggg', 'running', datetime('now'))")
    c.commit()
    c.close()
    _daemon(monkeypatch, tmp_path, "enrich")
    assert _claims(db) == [
        (1, "pending", None, "not_requested", None),
        (2, "pending", None, "not_requested", None),
        (3, "matched", None, "in_progress", "daemon:box:2:ccc"),
        (4, "in_progress", "daemon:live:4:ggg", "not_requested", None),
    ]


def test_shutdown_releases_own_work_claims(monkeypatch, tmp_path):
    db = tmp_path / "lit.db"
    _seed(db, jobs=[], heartbeats=[])
    daemon = _daemon(monkeypatch, tmp_path, "download")
    c = sqlite3.connect(db)
    c.execute("INSERT INTO works (id, title, metadata_status, download_status, download_claimed_by, download_lease_expires_at) VALUES (1, 'w', 'matched', 'in_progress', ?, 9999999999)", (daemon.worker_id,))
    c.execute("INSERT INTO works (id, title, metadata_status, download_status, download_claimed_by, download_lease_expires_at) VALUES (2, 'w', 'matched', 'in_progress', 'daemon:other:1:hhh', 9999999999)")
    c.commit()
    c.close()
    daemon._release_own_running_jobs()
    assert _claims(db) == [(1, "matched", None, "queued", None), (2, "matched", None, "in_progress", "daemon:other:1:hhh")]


def test_sigterm_is_turned_into_keyboard_interrupt(monkeypatch, tmp_path):
    daemon = _daemon(monkeypatch, tmp_path, "enrich")
    previous = signal.getsignal(signal.SIGTERM)
    try:
        daemon._install_signal_handlers()
        with pytest.raises(KeyboardInterrupt):
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(0.2)  # the handler runs at the next bytecode boundary
    finally:
        signal.signal(signal.SIGTERM, previous)
