import importlib.util
from pathlib import Path

import pytest

from dl_lit import app_settings
from dl_lit.utils import env_bool, env_float, env_int

WORKER = Path(__file__).resolve().parents[2] / "backend" / "scripts" / "daemon" / "worker.py"


@pytest.fixture(autouse=True)
def _reset_snapshot():
    from dl_lit.utils import reset_global_rate_limiter

    app_settings._ORIGINAL_ENV.clear()
    reset_global_rate_limiter()
    yield
    app_settings._ORIGINAL_ENV.clear()
    reset_global_rate_limiter()


def test_env_int_invalid_uses_default_and_warns(monkeypatch, capsys):
    monkeypatch.setenv("X_INT", "two")
    assert env_int("X_INT", 2, minimum=1) == 2
    err = capsys.readouterr().err
    assert "[env] X_INT=two is not a valid int; using 2" in err


def test_env_int_unset_or_empty_uses_default_silently(monkeypatch, capsys):
    monkeypatch.delenv("X_INT", raising=False)
    assert env_int("X_INT", 7) == 7
    monkeypatch.setenv("X_INT", "")
    assert env_int("X_INT", 7) == 7
    assert capsys.readouterr().err == ""


def test_env_int_clamps(monkeypatch):
    monkeypatch.setenv("X_INT", "0")
    assert env_int("X_INT", 2, minimum=1) == 1
    monkeypatch.setenv("X_INT", "99")
    assert env_int("X_INT", 2, maximum=10) == 10
    monkeypatch.setenv("X_INT", " 5 ")
    assert env_int("X_INT", 2, minimum=1, maximum=10) == 5
    monkeypatch.setenv("X_INT", "-3")
    assert env_int("X_INT", 2) == -3


def test_env_float_invalid_uses_default_and_warns(monkeypatch, capsys):
    monkeypatch.setenv("X_FLOAT", "1,5")
    assert env_float("X_FLOAT", 0.4, minimum=0.0) == 0.4
    assert "[env] X_FLOAT=1,5 is not a valid float; using 0.4" in capsys.readouterr().err
    monkeypatch.setenv("X_FLOAT", "-1.0")
    assert env_float("X_FLOAT", 0.4, minimum=0.0) == 0.0
    monkeypatch.setenv("X_FLOAT", "2.5")
    assert env_float("X_FLOAT", 0.4, maximum=2.0) == 2.0


def test_env_bool(monkeypatch):
    for v, want in [("1", True), ("YES", True), (" on ", True), ("True", True),
                    ("0", False), ("", False), ("maybe", False), ("off", False)]:
        monkeypatch.setenv("X_B", v)
        assert env_bool("X_B") is want
    monkeypatch.delenv("X_B")
    assert env_bool("X_B", True) is True
    assert env_bool("X_B") is False


def test_private_aliases_still_exported():
    from dl_lit import utils

    assert utils._env_int is utils.env_int
    assert utils._env_float is utils.env_float


def test_daemon_init_survives_env_typo(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(WORKER.parent))
    spec = importlib.util.spec_from_file_location("dt_worker_env", WORKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("RAG_FEEDER_LLM_PROVIDER", raising=False)
    monkeypatch.setattr(mod.PipelineDaemon, "_build_enhancer", lambda self, *a: object())
    monkeypatch.setenv("RAG_FEEDER_DAEMON_HEARTBEAT_SECONDS", "fast")
    monkeypatch.setenv("RAG_FEEDER_PIPELINE_JOB_TIMEOUT", "30min")
    daemon = mod.PipelineDaemon(str(tmp_path / "t.db"), role="enrich")
    assert daemon.heartbeat_interval_seconds == 3
    assert daemon.job_timeout_seconds == 1800


def test_enhancer_init_survives_env_typo(monkeypatch):
    from dl_lit import new_dl

    monkeypatch.setattr(new_dl, "get_client", lambda: None)
    monkeypatch.delenv("RAG_FEEDER_VPN_PROXY_URL", raising=False)
    monkeypatch.setenv("RAG_FEEDER_DOWNLOAD_TIMEOUT_SECONDS", "twelve")
    monkeypatch.setenv("RAG_FEEDER_VPN_STATUS_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("RAG_FEEDER_USE_UNPAYWALL", "off")
    enhancer = new_dl.BibliographyEnhancer(db_manager=None, rate_limiter=None, llm_optional=True)
    assert enhancer.download_timeout == 12
    assert enhancer.vpn_status_timeout_seconds == 2
    assert enhancer.enable_unpaywall is False
