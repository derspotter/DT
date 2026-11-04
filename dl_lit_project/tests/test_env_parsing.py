import os
import subprocess
import sys
from pathlib import Path

import pytest

from dl_lit import app_settings
from dl_lit.utils import env_bool, env_float, env_int
from tests._worker_import import load_worker

PROJECT_ROOT = Path(__file__).resolve().parents[1]


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
    mod = load_worker("dt_worker_env", monkeypatch)
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


# --- second pass: env_bool warns on an invalid value, env_float rejects nan/inf ---------------
def test_env_bool_blank_means_the_default(monkeypatch, capsys):
    for raw in ("", "   "):
        monkeypatch.setenv("X_B_BLANK", raw)
        assert env_bool("X_B_BLANK", True) is True
        assert env_bool("X_B_BLANK", False) is False
    assert capsys.readouterr().err == ""


def test_env_bool_invalid_value_warns_once_and_gives_the_default(monkeypatch, capsys):
    monkeypatch.setenv("X_B_TYPO", "ture")
    assert env_bool("X_B_TYPO", True) is True
    assert env_bool("X_B_TYPO", True) is True
    assert env_bool("X_B_TYPO", False) is False
    err = capsys.readouterr().err
    assert err.count("X_B_TYPO") == 1 and "ture" in err
    monkeypatch.setenv("X_B_TYPO", "tru")  # another value is another mistake
    assert env_bool("X_B_TYPO", True) is True
    assert capsys.readouterr().err.count("X_B_TYPO") == 1


@pytest.mark.parametrize("raw", ["0", "false", "No", " OFF "])
def test_env_bool_falsy_values_override_a_true_default_silently(monkeypatch, capsys, raw):
    monkeypatch.setenv("X_B_OFF", raw)
    assert env_bool("X_B_OFF", True) is False
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("raw", ["nan", "NaN", "inf", "-inf", "Infinity", "1e999"])
def test_env_float_treats_non_finite_values_as_invalid(monkeypatch, capsys, raw):
    monkeypatch.setenv("X_F_NF", raw)
    assert env_float("X_F_NF", 0.4, minimum=0.0) == 0.4
    assert "X_F_NF" in capsys.readouterr().err


# --- second pass: no inline float(os.getenv(...)) crash sites left ----------------------------
def test_searcher_timeouts_survive_a_typo(monkeypatch, capsys):
    from dl_lit.OpenAlexScraper import OpenAlexCrossrefSearcher

    monkeypatch.setenv("RAG_FEEDER_OPENALEX_TIMEOUT", "2o")
    monkeypatch.setenv("RAG_FEEDER_CROSSREF_TIMEOUT", "7.5")
    monkeypatch.delenv("RAG_FEEDER_SEMANTIC_SCHOLAR_TIMEOUT", raising=False)
    searcher = OpenAlexCrossrefSearcher()
    assert (searcher.openalex_timeout, searcher.crossref_timeout, searcher.semantic_scholar_timeout) == (20.0, 7.5, 20.0)
    assert "RAG_FEEDER_OPENALEX_TIMEOUT" in capsys.readouterr().err


def test_llm_provider_import_survives_a_typo():
    env = {**os.environ, "RAG_FEEDER_OPENAI_HTTP_TIMEOUT_SECONDS": "6oo", "PYTHONPATH": str(PROJECT_ROOT)}
    proc = subprocess.run(
        [sys.executable, "-c", "from dl_lit import llm_provider as p; print(p.HTTP_TIMEOUT_SECONDS)"],
        env=env, cwd=PROJECT_ROOT, capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip().splitlines()[-1] == "600.0"
    assert "RAG_FEEDER_OPENAI_HTTP_TIMEOUT_SECONDS" in proc.stderr
