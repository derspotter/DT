import importlib.util, sqlite3, sys
from pathlib import Path
WORKER = Path(__file__).resolve().parents[2] / "backend" / "scripts" / "daemon" / "worker.py"

def _load(monkeypatch):
    monkeypatch.syspath_prepend(str(WORKER.parent))
    spec = importlib.util.spec_from_file_location("dt_worker", WORKER)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod

def test_refresh_rebuilds_enhancer_only_when_settings_change(monkeypatch, tmp_path):
    mod = _load(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("RAG_FEEDER_LLM_PROVIDER", raising=False)
    builds = []
    monkeypatch.setattr(mod.PipelineDaemon, "_build_enhancer", lambda self: builds.append(1) or object())
    daemon = mod.PipelineDaemon(str(tmp_path / "lit.db"), role="enrich")
    assert len(builds) == 1                      # built once at construction
    assert daemon.refresh_app_settings() is False
    c = sqlite3.connect(tmp_path / "lit.db")
    c.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT, updated_by_user_id INTEGER, updated_at TIMESTAMP)")
    c.execute("INSERT INTO app_settings (key, value) VALUES ('llm_provider', 'openai')"); c.commit(); c.close()
    assert daemon.refresh_app_settings() is True
    assert len(builds) == 2
    assert daemon.refresh_app_settings() is False   # unchanged -> no rebuild
