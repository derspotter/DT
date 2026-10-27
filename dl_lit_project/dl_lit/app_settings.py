"""Resolve Admin-tab settings (app_settings table) into environment variables."""
import hashlib
import os
import sqlite3
from pathlib import Path

# Mirrors APP_SETTING_DEFS in backend/src/app.js.
APP_SETTING_ENV: dict[str, str] = {
    "openalex_api_key": "OPENALEX_API_KEY",
    "openalex_rps": "RAG_FEEDER_OPENALEX_RPS",
    "search_warn_threshold": "RAG_FEEDER_SEARCH_WARN_THRESHOLD",
    "llm_provider": "RAG_FEEDER_LLM_PROVIDER",
    "openai_base_url": "RAG_FEEDER_OPENAI_BASE_URL",
    "openai_api_key": "OPENAI_API_KEY",
    "gemini_api_key": "GEMINI_API_KEY",
    "extract_model": "RAG_FEEDER_API_EXTRACT_MODEL",
    "openai_model": "RAG_FEEDER_OPENAI_MODEL",
    "gemini_model": "RAG_FEEDER_GEMINI_MODEL",
}


def load_app_settings_env(db_path) -> dict[str, str]:
    """Return env name -> stored non-blank value; {} when the table is missing."""
    uri = f"file:{Path(db_path).resolve()}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=3.0)
        try:
            rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
        finally:
            conn.close()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            return {}
        raise
    env: dict[str, str] = {}
    for key, value in rows:
        name = APP_SETTING_ENV.get(key)
        text = str(value).strip() if value is not None else ""
        if name and text:
            env[name] = text
    return env


# Original process values of the managed env names, captured on first apply so
# that clearing a setting restores them instead of leaving a stale override.
_ORIGINAL_ENV: dict[str, str | None] = {}


def apply_app_settings_env(db_path) -> dict[str, str]:
    env = load_app_settings_env(db_path)
    for name in APP_SETTING_ENV.values():
        _ORIGINAL_ENV.setdefault(name, os.environ.get(name))
        value = env.get(name, _ORIGINAL_ENV[name])
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
    return env


def settings_fingerprint(env: dict[str, str]) -> str:
    if not env:
        return ""
    return hashlib.sha256(repr(sorted(env.items())).encode("utf-8")).hexdigest()


def effective_env() -> dict[str, str]:
    """Current process values of the managed env names (stored override or original)."""
    return {n: os.environ[n] for n in APP_SETTING_ENV.values() if os.environ.get(n)}
