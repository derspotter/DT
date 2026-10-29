import importlib.util
import sqlite3
from pathlib import Path

PROBE = Path(__file__).resolve().parents[2] / "backend" / "scripts" / "timing_probe.py"


def _load(monkeypatch):
    monkeypatch.syspath_prepend(str(PROBE.parent))
    spec = importlib.util.spec_from_file_location("timing_probe_script", PROBE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_probe_year_udf_matches_normalize_year_and_survives_text_years(monkeypatch, tmp_path):
    path = tmp_path / "lit.db"
    sqlite3.connect(path).close()
    mod = _load(monkeypatch)
    conn = mod.connect(str(path))
    try:
        row = conn.execute(
            "SELECT dt_corpus_int('n.d.'), dt_corpus_int(2005.5), dt_corpus_int('2005'), dt_corpus_int('2005.5'), dt_corpus_int(NULL)"
        ).fetchone()
    finally:
        conn.close()
    assert row == (0, 2005, 2005, 0, 0)
