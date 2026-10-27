import os
import sqlite3

import pytest
from dl_lit import app_settings

@pytest.fixture(autouse=True)
def _reset_snapshot():
    app_settings._ORIGINAL_ENV.clear()
    yield
    app_settings._ORIGINAL_ENV.clear()

def _db(tmp_path, rows):
    p = tmp_path / "auth.db"
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT, updated_by_user_id INTEGER, updated_at TIMESTAMP)")
    c.executemany("INSERT INTO app_settings (key, value) VALUES (?, ?)", rows)
    c.commit()
    c.close()
    return p

def test_env_map_matches_backend_defs():
    assert app_settings.APP_SETTING_ENV["llm_provider"] == "RAG_FEEDER_LLM_PROVIDER"
    assert app_settings.APP_SETTING_ENV["openalex_api_key"] == "OPENALEX_API_KEY"
    assert len(app_settings.APP_SETTING_ENV) == 10

def test_stored_value_wins_over_env(tmp_path, monkeypatch):
    monkeypatch.setenv("RAG_FEEDER_LLM_PROVIDER", "gemini")
    db = _db(tmp_path, [("llm_provider", "openai")])
    assert app_settings.apply_app_settings_env(db) == {"RAG_FEEDER_LLM_PROVIDER": "openai"}
    assert os.environ["RAG_FEEDER_LLM_PROVIDER"] == "openai"

def test_blank_stored_value_does_not_override_env(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "from-env")
    db = _db(tmp_path, [("openai_api_key", "   "), ("unknown_key", "x")])
    assert app_settings.load_app_settings_env(db) == {}
    app_settings.apply_app_settings_env(db)
    assert os.environ["OPENAI_API_KEY"] == "from-env"

def test_missing_table_is_empty(tmp_path):
    p = tmp_path / "empty.db"
    sqlite3.connect(p).close()
    assert app_settings.load_app_settings_env(p) == {}

def test_fingerprint_is_order_independent_and_empty_for_nothing():
    a = app_settings.settings_fingerprint({"A": "1", "B": "2"})
    assert a == app_settings.settings_fingerprint({"B": "2", "A": "1"})
    assert a != app_settings.settings_fingerprint({"A": "1"})
    assert app_settings.settings_fingerprint({}) == ""

def test_cleared_setting_restores_original_env(tmp_path, monkeypatch):
    monkeypatch.setenv("RAG_FEEDER_LLM_PROVIDER", "gemini")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    db = _db(tmp_path, [("llm_provider", "openai"), ("openai_api_key", "k")])
    app_settings.apply_app_settings_env(db)
    assert os.environ["RAG_FEEDER_LLM_PROVIDER"] == "openai"
    c = sqlite3.connect(db)
    c.execute("DELETE FROM app_settings")
    c.commit()
    c.close()
    assert app_settings.apply_app_settings_env(db) == {}
    assert os.environ["RAG_FEEDER_LLM_PROVIDER"] == "gemini"
    assert "OPENAI_API_KEY" not in os.environ

def test_locked_database_propagates(tmp_path, monkeypatch):
    db = _db(tmp_path, [])
    def boom(*a, **k):
        raise sqlite3.OperationalError("database is locked")
    monkeypatch.setattr(app_settings.sqlite3, "connect", boom)
    with pytest.raises(sqlite3.OperationalError):
        app_settings.load_app_settings_env(db)
