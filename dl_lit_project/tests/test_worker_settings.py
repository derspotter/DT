import importlib.util
import sqlite3
from pathlib import Path

import pytest

from dl_lit import app_settings

WORKER = Path(__file__).resolve().parents[2] / "backend" / "scripts" / "daemon" / "worker.py"


@pytest.fixture(autouse=True)
def _reset_snapshot():
    from dl_lit.utils import reset_global_rate_limiter

    app_settings._ORIGINAL_ENV.clear()
    reset_global_rate_limiter()
    yield
    app_settings._ORIGINAL_ENV.clear()
    reset_global_rate_limiter()


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
    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self, *a: builds.append(1) or object())
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

    def build(self, *a):
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
    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self, *a: builds.append(1) or object())
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
    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self, *a: object())
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
    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self, *a: builds.append(1) or object(), provider="openai")
    _set_setting(tmp_path / "lit.db", "llm_provider", "openai")
    assert daemon.refresh_app_settings() is False
    assert len(builds) == 1


def test_persistent_rebuild_failure_claims_no_job_and_backs_off(monkeypatch, tmp_path):
    state = {"fail": False}

    def build(self, *a):
        if state["fail"]:
            raise RuntimeError("no model configured")
        return object()

    mod, daemon = _daemon(monkeypatch, tmp_path, build)
    ids = [daemon.enqueue_job(None, "enrich", {}) for _ in range(2)]
    _set_setting(tmp_path / "lit.db", "llm_provider", "openai")
    state["fail"] = True
    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= 3:
            daemon.running = False

    monkeypatch.setattr(mod.time, "sleep", fake_sleep)
    daemon.run()
    statuses = [
        daemon.db.conn.execute("SELECT status FROM pipeline_jobs WHERE id = ?", (i,)).fetchone()[0]
        for i in ids
    ]
    assert statuses == ["pending", "pending"]
    assert sleeps and all(s >= 5 for s in sleeps)


def test_failed_refresh_in_direct_cycle_leaks_no_heartbeat_thread(monkeypatch, tmp_path):
    import threading

    mod, daemon = _daemon(monkeypatch, tmp_path, lambda self, *a: object())
    baseline = threading.active_count()

    def boom(self, *a, **k):
        raise RuntimeError("direct cycle failed")

    # The direct cycle (not the refresh, which run() does before claiming) raises.
    monkeypatch.setattr(mod.PipelineDaemon, "do_enrich", boom)
    monkeypatch.setattr(mod.PipelineDaemon, "fetch_next_job", lambda self, *a: None)

    def stop(_seconds):
        daemon.running = False

    monkeypatch.setattr(mod.time, "sleep", stop)
    daemon.run()
    assert threading.active_count() == baseline
    status = daemon.heartbeat_conn.execute(
        "SELECT status FROM daemon_heartbeat WHERE daemon_name = ?", (daemon.daemon_name,)
    ).fetchone()[0]
    assert status == "error"


def test_half_configured_llm_provider_does_not_halt_worker(monkeypatch, tmp_path):
    """F1: openai without key/model must not raise; the enhancer is rebuilt without a client."""
    from dl_lit import llm_provider

    mod = _load(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(llm_provider, "load_dotenv", lambda *a, **k: None)
    for name in ("RAG_FEEDER_LLM_PROVIDER", "OPENAI_API_KEY", "RAG_FEEDER_OPENAI_MODEL", "GEMINI_API_KEY", "GOOGLE_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    daemon = mod.PipelineDaemon(str(tmp_path / "lit.db"), role="enrich")
    first = daemon.enhancer
    job_id = daemon.enqueue_job(None, "enrich", {})
    _set_setting(tmp_path / "lit.db", "llm_provider", "openai")
    assert daemon.refresh_app_settings() is True
    assert daemon.enhancer is not first
    assert daemon.enhancer.client is None
    processed = []
    monkeypatch.setattr(
        mod.PipelineDaemon, "do_enrich",
        lambda self, *a, **k: processed.append(a) or {"processed": 0},
    )

    def stop(_seconds):
        daemon.running = False

    monkeypatch.setattr(mod.time, "sleep", stop)
    orig_process = mod.PipelineDaemon._process_job

    def process_once(self, job):
        orig_process(self, job)
        self.running = False

    monkeypatch.setattr(mod.PipelineDaemon, "_process_job", process_once)
    daemon.run()
    status = daemon.db.conn.execute("SELECT status FROM pipeline_jobs WHERE id = ?", (job_id,)).fetchone()[0]
    assert status == "completed"
    assert processed


def test_failed_build_publishes_nothing_and_rollback_is_noop(monkeypatch, tmp_path):
    """F2: limiter, searcher and enhancer are published together, or not at all."""
    from dl_lit.utils import get_global_rate_limiter

    monkeypatch.delenv("RAG_FEEDER_OPENALEX_RPS", raising=False)
    state = {"fail": False}
    builds = []

    def build(self, *a):
        if state["fail"]:
            raise RuntimeError("bad key")
        builds.append(1)
        return object()

    mod, daemon = _daemon(monkeypatch, tmp_path, build)
    searcher, limiter, enhancer = daemon.searcher, daemon.rate_limiter, daemon.enhancer
    fp = daemon._settings_fingerprint
    assert get_global_rate_limiter() is limiter
    _set_setting(tmp_path / "lit.db", "openalex_rps", "7")
    state["fail"] = True
    with pytest.raises(RuntimeError):
        daemon.refresh_app_settings()
    assert daemon.searcher is searcher
    assert daemon.rate_limiter is limiter
    assert daemon.enhancer is enhancer
    assert daemon._settings_fingerprint == fp
    assert get_global_rate_limiter() is limiter
    # Roll the stored setting back to the original (unset) value: nothing changed, so a no-op.
    c = sqlite3.connect(tmp_path / "lit.db")
    c.execute("DELETE FROM app_settings WHERE key = 'openalex_rps'")
    c.commit()
    c.close()
    state["fail"] = False
    assert daemon.refresh_app_settings() is False
    assert daemon.rate_limiter is limiter
    assert daemon.rate_limiter.service_config["openalex"]["limit"] == 30
    assert len(builds) == 1
