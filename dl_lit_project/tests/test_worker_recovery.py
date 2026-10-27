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


def _seed(path, jobs, heartbeats):
    c = sqlite3.connect(path)
    c.executescript(SCHEMA)
    for job_type, worker_id, started in jobs:
        c.execute(
            "INSERT INTO pipeline_jobs (job_type, status, worker_id, started_at) VALUES (?, 'running', ?, ?)",
            (job_type, worker_id, started),
        )
    for name, worker_id, seen in heartbeats:
        c.execute(
            "INSERT INTO daemon_heartbeat (daemon_name, worker_id, status, last_seen_at) VALUES (?, ?, 'running', ?)",
            (name, worker_id, seen),
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
            ("enrich", "daemon:box:1", "2026-10-27 10:00:00"),      # predecessor (same daemon_name, fresh heartbeat)
            ("enrich", "daemon:elsewhere:7", "2026-10-27 10:00:00"),  # no heartbeat at all
            ("download", "daemon:box:2", "2026-10-27 10:00:00"),     # other role: not ours to touch
            ("enrich", "daemon:pipe:3", None),                       # claimed by a live role-all daemon, fresh
        ],
        heartbeats=[
            ("enrich", "daemon:box:1", "2026-10-27 10:00:05"),
            ("download", "daemon:box:2", "2026-10-27 10:00:05"),
        ],
    )
    # Make the role-all heartbeat genuinely fresh and the job recent.
    c = sqlite3.connect(db)
    c.execute("INSERT INTO daemon_heartbeat (daemon_name, worker_id, status, last_seen_at) VALUES ('pipeline', 'daemon:pipe:3', 'running', datetime('now'))")
    c.execute("UPDATE pipeline_jobs SET started_at = datetime('now') WHERE worker_id = 'daemon:pipe:3'")
    c.commit()
    c.close()

    _daemon(monkeypatch, tmp_path, "enrich")

    assert _statuses(db) == [
        ("enrich", None, "pending"),
        ("enrich", None, "pending"),
        ("download", "daemon:box:2", "running"),
        ("enrich", "daemon:pipe:3", "running"),
    ]


def test_download_startup_requeues_only_download_jobs(monkeypatch, tmp_path):
    db = tmp_path / "lit.db"
    _seed(
        db,
        jobs=[("download", "daemon:box:2", None), ("enrich", "daemon:gone:1", None)],
        heartbeats=[("download", "daemon:box:2", "2026-10-27 10:00:05")],
    )
    _daemon(monkeypatch, tmp_path, "download")
    assert _statuses(db) == [("download", None, "pending"), ("enrich", "daemon:gone:1", "running")]


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
