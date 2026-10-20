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


def _daemon(monkeypatch, tmp_path, builder):
    mod = _load(monkeypatch)
    monkeypatch.chdir(tmp_path)
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
