import importlib.util
import sqlite3
from pathlib import Path

import pytest

from dl_lit import app_settings

WORKER = Path(__file__).resolve().parents[2] / "backend" / "scripts" / "daemon" / "worker.py"


@pytest.fixture(autouse=True)
def _reset_snapshot():
    app_settings._ORIGINAL_ENV.clear()
    yield
    app_settings._ORIGINAL_ENV.clear()


def _load(monkeypatch):
    monkeypatch.syspath_prepend(str(WORKER.parent))
    spec = importlib.util.spec_from_file_location("dt_worker", WORKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _set_setting(path, key, value):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT, updated_by_user_id INTEGER, updated_at TIMESTAMP)")
    c.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)", (key, value))
    c.commit()
    c.close()


def _daemon(monkeypatch, tmp_path, builder, provider=None):
    mod = _load(monkeypatch)
    monkeypatch.chdir(tmp_path)
    if provider:
        monkeypatch.setenv("RAG_FEEDER_LLM_PROVIDER", provider)
    else:
        monkeypatch.delenv("RAG_FEEDER_LLM_PROVIDER", raising=False)
    monkeypatch.setattr(mod.PipelineDaemon, "_build_enhancer", builder)
    return mod, mod.PipelineDaemon(str(tmp_path / "lit.db"), role="enrich")


def test_refresh_rebuilds_enhancer_only_when_settings_change(monkeypatch, tmp_path):
    builds = []
    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self: builds.append(1) or object())
    assert len(builds) == 1
    first = daemon.enhancer
    assert daemon.refresh_app_settings() is False
    _set_setting(tmp_path / "lit.db", "llm_provider", "openai")
    assert daemon.refresh_app_settings() is True
    assert len(builds) == 2
    assert daemon.enhancer is not first
    assert daemon.refresh_app_settings() is False


def test_failed_rebuild_is_retried(monkeypatch, tmp_path):
    state = {"fail": False}

    def build(self):
        if state["fail"]:
            raise RuntimeError("bad key")
        return object()

    mod, daemon = _daemon(monkeypatch, tmp_path, build)
    old_fp, old_enh = daemon._settings_fingerprint, daemon.enhancer
    _set_setting(tmp_path / "lit.db", "llm_provider", "openai")
    state["fail"] = True
    with pytest.raises(RuntimeError):
        daemon.refresh_app_settings()
    assert daemon._settings_fingerprint == old_fp
    assert daemon.enhancer is old_enh
    state["fail"] = False
    assert daemon.refresh_app_settings() is True
    assert daemon.enhancer is not old_enh


def test_locked_database_keeps_previous_state(monkeypatch, tmp_path):
    builds = []
    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self: builds.append(1) or object())
    fp = daemon._settings_fingerprint

    def boom(*a, **k):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(mod, "apply_app_settings_env", boom)
    assert daemon.refresh_app_settings() is False
    assert daemon._settings_fingerprint == fp
    assert len(builds) == 1


def test_openalex_consumers_follow_settings(monkeypatch, tmp_path):
    from dl_lit.utils import get_global_rate_limiter

    monkeypatch.delenv("OPENALEX_API_KEY", raising=False)
    monkeypatch.delenv("RAG_FEEDER_OPENALEX_RPS", raising=False)
    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self: object())
    old_searcher, old_limiter = daemon.searcher, daemon.rate_limiter
    assert old_searcher.api_key is None
    _set_setting(tmp_path / "lit.db", "openalex_api_key", "new-key")
    _set_setting(tmp_path / "lit.db", "openalex_rps", "7")
    assert daemon.refresh_app_settings() is True
    assert daemon.searcher is not old_searcher
    assert daemon.searcher.api_key == "new-key"
    assert daemon.rate_limiter is not old_limiter
    assert get_global_rate_limiter() is daemon.rate_limiter
    assert daemon.rate_limiter.service_config["openalex"]["limit"] == 7


def test_saving_value_equal_to_env_does_not_rebuild(monkeypatch, tmp_path):
    builds = []
    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self: builds.append(1) or object(), provider="openai")
    _set_setting(tmp_path / "lit.db", "llm_provider", "openai")
    assert daemon.refresh_app_settings() is False
    assert len(builds) == 1


def test_failed_rebuild_marks_claimed_job_failed(monkeypatch, tmp_path):
    state = {"fail": False}

    def build(self):
        if state["fail"]:
            raise RuntimeError("no model configured")
        return object()

    mod, daemon = _daemon(monkeypatch, tmp_path, build)
    job_id = daemon.enqueue_job(None, "enrich", {})
    job = daemon.fetch_next_job()
    assert job["id"] == job_id
    _set_setting(tmp_path / "lit.db", "llm_provider", "openai")
    state["fail"] = True
    daemon._process_job(job)
    row = daemon.db.conn.execute(
        "SELECT status, result_json FROM pipeline_jobs WHERE id = ?", (job_id,)
    ).fetchone()
    assert row[0] == "failed"
    assert "no model configured" in row[1]


def test_failed_refresh_in_direct_cycle_leaks_no_heartbeat_thread(monkeypatch, tmp_path):
    import threading

    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self: object())
    baseline = threading.active_count()

    def boom(self):
        raise RuntimeError("bad key")

    monkeypatch.setattr(mod.PipelineDaemon, "refresh_app_settings", boom)
    monkeypatch.setattr(mod.PipelineDaemon, "fetch_next_job", lambda self: None)

    def stop(_seconds):
        daemon.running = False

    monkeypatch.setattr(mod.time, "sleep", stop)
    daemon.run()
    assert threading.active_count() == baseline
    status = daemon.heartbeat_conn.execute(
        "SELECT status FROM daemon_heartbeat WHERE daemon_name = ?", (daemon.daemon_name,)
    ).fetchone()[0]
    assert status == "error"
